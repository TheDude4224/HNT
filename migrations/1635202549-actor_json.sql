-- migrations/1635202549-actor_json.sql
-- :up

alter table transaction_actors add column fields jsonb;

CREATE OR REPLACE FUNCTION gateway_inventory_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE gateway_inventory SET payer = (
        select t.fields->>'payer' 
        from transaction_actors a inner join transactions t 
        on a.transaction_hash = t.hash
            and a.actor = NEW.address
            and a.actor_role = 'gateway'
            and a.block = NEW.first_block
        limit 1
    )
    where address = NEW.address;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- :down

alter table transaction_actors drop column fields;
