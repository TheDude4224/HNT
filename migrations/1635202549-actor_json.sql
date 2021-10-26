-- migrations/1635202549-actor_json.sql
-- :up

alter table transaction_actors add column fields jsonb;


-- :down

alter table transaction_actors drop column fields;
