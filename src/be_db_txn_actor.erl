-module(be_db_txn_actor).

-include("be_db_follower.hrl").
-include("be_db_worker.hrl").

-include_lib("helium_proto/include/blockchain_txn_rewards_v2_pb.hrl").

-behavior(be_db_worker).
-behavior(be_db_follower).

%% be_db_worker
-export([prepare_conn/1]).
%% be_block_handler
-export([init/1, load_block/6]).
%% api
-export([txn_actors/2, q_insert_transaction_actors/3]).

-define(S_INSERT_ACTOR, "insert_actor").

-record(state, {}).

%%
%% be_db_worker
%%

prepare_conn(Conn) ->
    {ok, S1} =
        epgsql:parse(
            Conn,
            ?S_INSERT_ACTOR,
            [
                "insert into transaction_actors (block, actor, actor_role, transaction_hash, fields) ",
                "values ($1, $2, $3, $4, $5) ",
                "on conflict do nothing"
            ],
            []
        ),

    #{
        ?S_INSERT_ACTOR => S1
    }.

%%
%% be_block_handler
%%

init(_) ->
    {ok, #state{}}.

load_block(Conn, _Hash, Block, _Sync, _Ledger, State = #state{}) ->
    Queries = q_insert_block_transaction_actors(Block, fun be_db_block:txn_json/1),
    ok = ?BATCH_QUERY(Conn, Queries),
    {ok, State}.

q_insert_transaction_actors(Height, Txn, TxnJsonFun) ->
    TxnHash = ?BIN_TO_B64(blockchain_txn:hash(Txn)),
    lists:map(
        fun({Role, Address, Fields}) ->
            [
                {?S_INSERT_ACTOR, [Height, Address, Role, TxnHash, Fields]}
            ]
        end,
        txn_actors(Txn, TxnJsonFun)
    ).

q_insert_block_transaction_actors(Block, TxnJsonFun) ->
    Height = blockchain_block_v1:height(Block),
    Txns = blockchain_block_v1:transactions(Block),
    TxnQueries = blockchain_utils:pmap(
        fun(Txn) ->
            q_insert_transaction_actors(Height, Txn, TxnJsonFun)
        end,
        Txns
    ),
    lists:flatten(TxnQueries).

-spec txn_actors(
    blockchain_txn:txn(),
    TxnJsonFun :: fun((TxnHash :: binary()) -> TxnJson :: map()) | undefined
) ->
    [
        {Role :: string(), Address :: libp2p_crypto:pubkey_bin()}
        | {Role :: string(), Address :: libp2p_crypto:pubkey_bin(), Fields :: map()}
    ].
txn_actors(T, TxnJsonFun) ->
    TxnType = blockchain_txn:type(T),
    TxnHash = blockchain_txn:hash(T),
    %% This is a terrible side effect hack which looks up the json that the
    %% be_db_block constructed.
    TxnActors = to_actors(TxnType, T),
    case TxnJsonFun of
        undefined ->
            TxnActors;
        _ ->
            TxnJson = TxnJsonFun(TxnHash),
            TxnFieldsCtx = prepare_actor_fields_ctx(TxnType, TxnJson),
            lists:map(
                fun({Role, Address}) ->
                    {Role, Address, to_actor_fields(TxnType, Role, Address, TxnJson, TxnFieldsCtx)}
                end,
                TxnActors
            )
    end.

prepare_actor_fields_ctx(Type, #{<<"rewards">> := Rewards}) when
    Type == blockchain_txn_reward_v1 orelse Type == blockchain_txn_rewards_v2
->
    {Gateways, Accounts} = lists:foldl(
        fun(Reward, {GatewayAcc, AccountAcc}) ->
            GWAcc =
                case maps:get(<<"gateway">>, Reward, undefined) of
                    undefined ->
                        GatewayAcc;
                    Gateway ->
                        maps:update_with(Gateway, fun(X) -> [Reward | X] end, [Reward], GatewayAcc)
                end,
            ACAcc =
                case maps:get(<<"acccount">>, Reward, undefined) of
                    undefined ->
                        AccountAcc;
                    Account ->
                        maps:update_with(Account, fun(X) -> [Reward | X] end, [Reward], AccountAcc)
                end,
            {GWAcc, ACAcc}
        end,
        {[], []},
        Rewards
    ),
    #{gateways => Gateways, accounts => Accounts};
prepare_actor_fields_ctx(blockchain_txn_state_channel_close_v1, #{
    <<"state_channel">> := StateChannel
}) ->
    Clients = lists:foldl(
        fun(Summary = #{<<"client">> := Client}, Acc) ->
            maps:update_with(Client, fun(X) -> [Summary | X] end, [Summary], Acc)
        end,
        #{},
        maps:get(<<"summaries">>, StateChannel, [])
    ),
    #{clients => Clients};
prepare_actor_fields_ctx(blockchain_txn_payment_v2, Fields) ->
    Payments = lists:foldl(
        fun(Payment = #{<<"payee">> := Payee}, Acc) ->
            maps:update_with(Payee, fun(X) -> [Payment | X] end, [Payment], Acc)
        end,
        #{},
        maps:get(<<"payments">>, Fields, [])
    ),
    #{payments => Payments};
prepare_actor_fields_ctx(_Type, _Fields) ->
    #{}.

to_actor_fields(Type, Role, Actor, Fields, #{gateways := Gateways, accounts := Accounts}) when
    Type == blockchain_txn_reward_v1 orelse Type == blockchain_txn_rewards_v2
->
    Rewards =
        case Role of
            "reward_gateway" -> maps:get(Actor, Gateways, []);
            "payee" -> maps:get(Actor, Accounts, [])
        end,
    Fields#{<<"rewards">> => Rewards};
to_actor_fields(blockchain_txn_state_channel_close_v1, Role, Actor, Fields, #{clients := Clients}) ->
    Summaries =
        case Role of
            "packet_receiver" -> maps:get(Actor, Clients, []);
            _ -> []
        end,
    StateChannel = maps:get(<<"state_channel">>, Fields, #{}),
    Fields#{<<"state_channel">> => StateChannel#{<<"summaries">> => Summaries}};
to_actor_fields(blockchain_txn_payment_v2, Role, Actor, Fields, #{payments := Payments}) ->
    Payments =
        case Role of
            "payer" -> [];
            "payee" -> maps:get(Actor, Payments, [])
        end,
    Fields#{<<"payments">> => Payments};
to_actor_fields(_Type, _Role, _Actor, Fields, _Ctx) ->
    Fields.

-spec to_actors(Type :: atom(), Txn :: blockchain_txn:txn()) ->
    [{Role :: string(), Address :: string()}].
to_actors(blockchain_txn_coinbase_v1, T) ->
    [{"payee", ?BIN_TO_B58(blockchain_txn_coinbase_v1:payee(T))}];
to_actors(blockchain_txn_security_coinbase_v1, T) ->
    [{"payee", ?BIN_TO_B58(blockchain_txn_security_coinbase_v1:payee(T))}];
to_actors(blockchain_txn_oui_v1, T) ->
    Routers = [{"router", R} || R <- blockchain_txn_oui_v1:addresses(T)],
    [
        {"owner", ?BIN_TO_B58(blockchain_txn_oui_v1:owner(T))},
        {"payer", ?BIN_TO_B58(blockchain_txn_oui_v1:payer(T))}
    ] ++ Routers;
to_actors(blockchain_txn_gen_gateway_v1, T) ->
    [
        {"gateway", ?BIN_TO_B58(blockchain_txn_gen_gateway_v1:gateway(T))},
        {"owner", ?BIN_TO_B58(blockchain_txn_gen_gateway_v1:owner(T))}
    ];
to_actors(blockchain_txn_routing_v1, T) ->
    Routers =
        case blockchain_txn_routing_v1:action(T) of
            {update_routers, Addrs} -> [{"router", ?BIN_TO_B58(R)} || R <- Addrs];
            _ -> []
        end,
    [
        {"owner", ?BIN_TO_B58(blockchain_txn_routing_v1:owner(T))},
        {"payer", ?BIN_TO_B58(blockchain_txn_routing_v1:owner(T))}
    ] ++ Routers;
to_actors(blockchain_txn_payment_v1, T) ->
    [
        {"payer", ?BIN_TO_B58(blockchain_txn_payment_v1:payer(T))},
        {"payee", ?BIN_TO_B58(blockchain_txn_payment_v1:payee(T))}
    ];
to_actors(blockchain_txn_security_exchange_v1, T) ->
    [
        {"payer", ?BIN_TO_B58(blockchain_txn_security_exchange_v1:payer(T))},
        {"payee", ?BIN_TO_B58(blockchain_txn_security_exchange_v1:payee(T))}
    ];
to_actors(blockchain_txn_consensus_group_v1, T) ->
    [{"consensus_member", ?BIN_TO_B58(M)} || M <- blockchain_txn_consensus_group_v1:members(T)];
to_actors(blockchain_txn_add_gateway_v1, T) ->
    Owner = blockchain_txn_add_gateway_v1:owner(T),
    Payer =
        case blockchain_txn_add_gateway_v1:payer(T) of
            undefined -> Owner;
            <<>> -> Owner;
            P -> P
        end,
    [
        {"gateway", ?BIN_TO_B58(blockchain_txn_add_gateway_v1:gateway(T))},
        {"owner", ?BIN_TO_B58(Owner)},
        {"payer", ?BIN_TO_B58(Payer)}
    ];
to_actors(blockchain_txn_assert_location_v1, T) ->
    Owner = blockchain_txn_assert_location_v1:owner(T),
    Payer =
        case blockchain_txn_assert_location_v1:payer(T) of
            undefined -> Owner;
            <<>> -> Owner;
            P -> P
        end,
    [
        {"gateway", ?BIN_TO_B58(blockchain_txn_assert_location_v1:gateway(T))},
        {"owner", ?BIN_TO_B58(Owner)},
        {"payer", ?BIN_TO_B58(Payer)}
    ];
to_actors(blockchain_txn_assert_location_v2, T) ->
    Owner = blockchain_txn_assert_location_v2:owner(T),
    Payer =
        case blockchain_txn_assert_location_v2:payer(T) of
            undefined -> Owner;
            <<>> -> Owner;
            P -> P
        end,
    [
        {"gateway", ?BIN_TO_B58(blockchain_txn_assert_location_v2:gateway(T))},
        {"owner", ?BIN_TO_B58(Owner)},
        {"payer", ?BIN_TO_B58(Payer)}
    ];
to_actors(blockchain_txn_create_htlc_v1, T) ->
    [
        {"payer", ?BIN_TO_B58(blockchain_txn_create_htlc_v1:payer(T))},
        {"payee", ?BIN_TO_B58(blockchain_txn_create_htlc_v1:payee(T))},
        {"escrow", ?BIN_TO_B58(blockchain_txn_create_htlc_v1:address(T))}
    ];
to_actors(blockchain_txn_redeem_htlc_v1, T) ->
    [
        {"payee", ?BIN_TO_B58(blockchain_txn_redeem_htlc_v1:payee(T))},
        {"escrow", ?BIN_TO_B58(blockchain_txn_redeem_htlc_v1:address(T))}
    ];
to_actors(blockchain_txn_poc_request_v1, T) ->
    [{"challenger", ?BIN_TO_B58(blockchain_txn_poc_request_v1:challenger(T))}];
to_actors(blockchain_txn_poc_receipts_v1, T) ->
    ToActors = fun
        (undefined, Acc0) ->
            Acc0;
        (Elem, {ChallengeeAcc0, WitnessAcc0}) ->
            ChallengeeAcc = [
                {"challengee", ?BIN_TO_B58(blockchain_poc_path_element_v1:challengee(Elem))}
                | ChallengeeAcc0
            ],
            WitnessAcc = lists:foldl(
                fun(W, WAcc) ->
                    [{"witness", ?BIN_TO_B58(blockchain_poc_witness_v1:gateway(W))} | WAcc]
                end,
                WitnessAcc0,
                blockchain_poc_path_element_v1:witnesses(Elem)
            ),
            {ChallengeeAcc, WitnessAcc}
    end,
    {Challengees, Witnesses} = lists:foldl(
        ToActors,
        {[], []},
        blockchain_txn_poc_receipts_v1:path(T)
    ),
    lists:usort(Challengees) ++
        lists:usort(Witnesses) ++
        [{"challenger", ?BIN_TO_B58(blockchain_txn_poc_receipts_v1:challenger(T))}];
to_actors(blockchain_txn_vars_v1, _T) ->
    [];
to_actors(blockchain_txn_rewards_v1, T) ->
    ToActors = fun(R, {PayeeAcc0, GatewayAcc0}) ->
        PayeeAcc = [{"payee", ?BIN_TO_B58(blockchain_txn_reward_v1:account(R))} | PayeeAcc0],
        GatewayAcc =
            case blockchain_txn_reward_v1:gateway(R) of
                undefined -> GatewayAcc0;
                G -> [{"reward_gateway", ?BIN_TO_B58(G)} | GatewayAcc0]
            end,
        {PayeeAcc, GatewayAcc}
    end,
    {Payees, Gateways} = lists:foldl(
        ToActors,
        {[], []},
        blockchain_txn_rewards_v1:rewards(T)
    ),
    lists:usort(Payees) ++ lists:usort(Gateways);
to_actors(blockchain_txn_rewards_v2, T) ->
    Start = blockchain_txn_rewards_v2:start_epoch(T),
    End = blockchain_txn_rewards_v2:end_epoch(T),
    %% rewards_v2 violates to_actor conventions by requiring the chain and
    %% ledger to construct it actors
    Chain = blockchain_worker:blockchain(),
    {ok, Ledger} = blockchain:ledger_at(End, Chain),
    {ok, Metadata} = be_db_reward:calculate_rewards_metadata(
        Start,
        End,
        Chain
    ),
    %% Take a rewards map for a category and add payees and reward_gateways to
    %% one or both of the payee or gateway accumulators
    ToActors = fun(Rewards, Acc) ->
        maps:fold(
            fun
                ({owner, _Type, O}, _Amt, {PayeeAcc, GatewayAcc}) ->
                    {[{"payee", ?BIN_TO_B58(O)} | PayeeAcc], GatewayAcc};
                ({gateway, _Type, G}, _Amt, {PayeeAcc, GatewayAcc}) ->
                    case blockchain_ledger_v1:find_gateway_owner(G, Ledger) of
                        {error, _Error} ->
                            {PayeeAcc, [{"reward_gateway", ?BIN_TO_B58(G)} | GatewayAcc]};
                        {ok, GwOwner} ->
                            {[{"payee", ?BIN_TO_B58(GwOwner)} | PayeeAcc], [
                                {"reward_gateway", ?BIN_TO_B58(G)}
                                | GatewayAcc
                            ]}
                    end;
                ({validator, _Type, V}, _Amt, {PayeeAcc, GatewayAcc}) ->
                    case blockchain_ledger_v1:get_validator(V, Ledger) of
                        {error, _Error} ->
                            {PayeeAcc, [{"validator", ?BIN_TO_B58(V)} | GatewayAcc]};
                        {ok, Validator} ->
                            Owner = blockchain_ledger_validator_v1:owner_address(Validator),
                            {[{"payee", ?BIN_TO_B58(Owner)} | PayeeAcc], [
                                {"validator", ?BIN_TO_B58(V)}
                                | GatewayAcc
                            ]}
                    end
            end,
            Acc,
            maps:iterator(Rewards)
        )
    end,
    %% Now fold over the metadata and call ToActors for each reward category
    {Payees, Gateways} = maps:fold(
        fun
            (overages, _Amount, Acc) ->
                Acc;
            (_RewardCategory, Rewards, Acc) ->
                ToActors(Rewards, Acc)
        end,
        {[], []},
        Metadata
    ),
    lists:usort(Payees) ++ lists:usort(Gateways);
to_actors(blockchain_txn_token_burn_v1, T) ->
    [
        {"payer", ?BIN_TO_B58(blockchain_txn_token_burn_v1:payer(T))},
        {"payee", ?BIN_TO_B58(blockchain_txn_token_burn_v1:payee(T))}
    ];
to_actors(blockchain_txn_dc_coinbase_v1, T) ->
    [{"payee", ?BIN_TO_B58(blockchain_txn_dc_coinbase_v1:payee(T))}];
to_actors(blockchain_txn_token_burn_exchange_rate_v1, _T) ->
    [];
to_actors(blockchain_txn_payment_v2, T) ->
    ToActors = fun(Payment, Acc) ->
        [{"payee", ?BIN_TO_B58(blockchain_payment_v2:payee(Payment))} | Acc]
    end,
    lists:foldl(
        ToActors,
        [{"payer", ?BIN_TO_B58(blockchain_txn_payment_v2:payer(T))}],
        blockchain_txn_payment_v2:payments(T)
    );
to_actors(blockchain_txn_state_channel_open_v1, T) ->
    %% TODO: In v1 state channels we're assuminig the the opener is
    %% the payer of the DC in the state channel.
    Opener = blockchain_txn_state_channel_open_v1:owner(T),
    [
        {"sc_opener", ?BIN_TO_B58(Opener)},
        {"payer", ?BIN_TO_B58(Opener)},
        {"owner", ?BIN_TO_B58(Opener)}
    ];
to_actors(blockchain_txn_state_channel_close_v1, T) ->
    %% NOTE: closer can be one of the clients of the state channel or the owner of the router
    %% if the state_channel expires
    SummaryToActors = fun(Summary, Acc) ->
        Receiver = blockchain_state_channel_summary_v1:client_pubkeybin(Summary),
        [{"packet_receiver", ?BIN_TO_B58(Receiver)} | Acc]
    end,
    Closer = blockchain_txn_state_channel_close_v1:closer(T),
    lists:foldl(
        SummaryToActors,
        %% TODO: In v1 state channels we're assuminig the the
        %% closer is the payee of any remaining DC in the
        %% state channel. This is not totally true since any
        %% client in the state channel can cause it to close
        %% to, but for v1 we expect this assumption to hold.
        [
            {"sc_closer", ?BIN_TO_B58(Closer)},
            {"payee", ?BIN_TO_B58(Closer)},
            {"owner", ?BIN_TO_B58(blockchain_txn_state_channel_close_v1:state_channel_owner(T))}
        ],
        blockchain_state_channel_v1:summaries(
            blockchain_txn_state_channel_close_v1:state_channel(T)
        )
    );
to_actors(blockchain_txn_gen_price_oracle_v1, _T) ->
    [];
to_actors(blockchain_txn_price_oracle_v1, T) ->
    [{"oracle", ?BIN_TO_B58(blockchain_txn_price_oracle_v1:public_key(T))}];
to_actors(blockchain_txn_transfer_hotspot_v1, T) ->
    [
        {"gateway", ?BIN_TO_B58(blockchain_txn_transfer_hotspot_v1:gateway(T))},
        {"payee", ?BIN_TO_B58(blockchain_txn_transfer_hotspot_v1:seller(T))},
        {"payer", ?BIN_TO_B58(blockchain_txn_transfer_hotspot_v1:buyer(T))},
        {"owner", ?BIN_TO_B58(blockchain_txn_transfer_hotspot_v1:buyer(T))}
    ];
to_actors(blockchain_txn_transfer_hotspot_v2, T) ->
    [
        {"gateway", ?BIN_TO_B58(blockchain_txn_transfer_hotspot_v2:gateway(T))},
        {"owner", ?BIN_TO_B58(blockchain_txn_transfer_hotspot_v2:new_owner(T))}
    ];
to_actors(blockchain_txn_gen_validator_v1, T) ->
    [
        {"validator", ?BIN_TO_B58(blockchain_txn_gen_validator_v1:address(T))},
        {"payer", ?BIN_TO_B58(blockchain_txn_gen_validator_v1:owner(T))},
        {"owner", ?BIN_TO_B58(blockchain_txn_gen_validator_v1:owner(T))}
    ];
to_actors(blockchain_txn_stake_validator_v1, T) ->
    [
        {"validator", ?BIN_TO_B58(blockchain_txn_stake_validator_v1:validator(T))},
        {"payer", ?BIN_TO_B58(blockchain_txn_stake_validator_v1:owner(T))},
        {"owner", ?BIN_TO_B58(blockchain_txn_stake_validator_v1:owner(T))}
    ];
to_actors(blockchain_txn_unstake_validator_v1, T) ->
    [
        {"validator", ?BIN_TO_B58(blockchain_txn_unstake_validator_v1:address(T))},
        {"payee", ?BIN_TO_B58(blockchain_txn_unstake_validator_v1:owner(T))},
        {"owner", ?BIN_TO_B58(blockchain_txn_unstake_validator_v1:owner(T))}
    ];
to_actors(blockchain_txn_transfer_validator_stake_v1, T) ->
    OldOwner = blockchain_txn_transfer_validator_stake_v1:old_owner(T),
    NewOwner = blockchain_txn_transfer_validator_stake_v1:new_owner(T),
    Owners =
        case NewOwner of
            OldOwner -> [{"owner", ?BIN_TO_B58(OldOwner)}];
            <<>> -> [{"owner", ?BIN_TO_B58(OldOwner)}];
            _ -> [{"owner", ?BIN_TO_B58(NewOwner)}, {"owner", ?BIN_TO_B58(OldOwner)}]
        end,
    [
        {"validator", ?BIN_TO_B58(blockchain_txn_transfer_validator_stake_v1:old_validator(T))},
        {"validator", ?BIN_TO_B58(blockchain_txn_transfer_validator_stake_v1:new_validator(T))},
        {"payer", ?BIN_TO_B58(blockchain_txn_transfer_validator_stake_v1:new_owner(T))},
        {"payee", ?BIN_TO_B58(blockchain_txn_transfer_validator_stake_v1:old_owner(T))}
    ] ++ Owners;
to_actors(blockchain_txn_validator_heartbeat_v1, T) ->
    [
        {"validator", ?BIN_TO_B58(blockchain_txn_validator_heartbeat_v1:address(T))}
    ];
to_actors(blockchain_txn_consensus_group_failure_v1, T) ->
    Members = [
        {"consensus_failure_member", ?BIN_TO_B58(M)}
     || M <- blockchain_txn_consensus_group_failure_v1:members(T)
    ],
    FailedMembers = [
        {"consensus_failure_failed_member", ?BIN_TO_B58(M)}
     || M <- blockchain_txn_consensus_group_failure_v1:failed_members(T)
    ],
    Members ++ FailedMembers.
