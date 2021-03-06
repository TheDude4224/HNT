-- migrations/1628075186-state_channel_close_v1_actitivy_coalesce.sql
-- :up

create or replace function txn_filter_actor_activity(actor text, type transaction_type, fields jsonb) returns jsonb as $$
begin
    case
        when type = 'rewards_v1' then
            return jsonb_set(fields, '{rewards}', (select jsonb_agg(x) from jsonb_to_recordset(fields#>'{rewards}') as x(account text, amount bigint, type text, gateway text) where account = actor or gateway = actor));
        when type = 'rewards_v2' then
            return jsonb_set(fields, '{rewards}', (select jsonb_agg(x) from jsonb_to_recordset(fields#>'{rewards}') as x(account text, amount bigint, type text, gateway text) where account = actor or gateway = actor));
        when type = 'state_channel_close_v1' then
            return jsonb_set(fields, '{state_channel,summaries}', coalesce((select jsonb_agg(x) from jsonb_to_recordset(fields#>'{state_channel,summaries}') as x(owner text, num_packets bigint, num_dcs bigint, location text, client text) where owner = actor or client = actor), '[]'));
        when type = 'payment_v2' then
            if fields->>'payer' = actor then
                return fields;
            else
                return jsonb_set(fields, '{payments}', (select jsonb_agg(x) from jsonb_to_recordset(fields#>'{payments}') as x(payee text, amount bigint) where payee = actor));
            end if;
        when type = 'consensus_group_v1' then
           return fields - 'proof';
        else
            return fields;
    end case;
end; $$
language plpgsql;

-- :down

create or replace function txn_filter_actor_activity(actor text, type transaction_type, fields jsonb) returns jsonb as $$
begin
    case
        when type = 'rewards_v1' then
            return jsonb_set(fields, '{rewards}', (select jsonb_agg(x) from jsonb_to_recordset(fields#>'{rewards}') as x(account text, amount bigint, type text, gateway text) where account = actor or gateway = actor));
        when type = 'rewards_v2' then
            return jsonb_set(fields, '{rewards}', (select jsonb_agg(x) from jsonb_to_recordset(fields#>'{rewards}') as x(account text, amount bigint, type text, gateway text) where account = actor or gateway = actor));
        when type = 'state_channel_close_v1' then
            return jsonb_set(fields, '{state_channel,summaries}', (select jsonb_agg(x) from jsonb_to_recordset(fields#>'{state_channel,summaries}') as x(owner text, num_packets bigint, num_dcs bigint, location text, client text) where owner = actor or client = actor));
        when type = 'payment_v2' then
            if fields->>'payer' = actor then
                return fields;
            else
                return jsonb_set(fields, '{payments}', (select jsonb_agg(x) from jsonb_to_recordset(fields#>'{payments}') as x(payee text, amount bigint) where payee = actor));
            end if;
        when type = 'consensus_group_v1' then
           return fields - 'proof';
        else
            return fields;
    end case;
end; $$
language plpgsql;