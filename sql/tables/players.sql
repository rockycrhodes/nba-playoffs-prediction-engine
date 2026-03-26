-- Table: public.players

-- DROP TABLE IF EXISTS public.players;

CREATE TABLE IF NOT EXISTS public.players
(
    player_id integer NOT NULL,
    player_name text COLLATE pg_catalog."default",
    first_name text COLLATE pg_catalog."default",
    last_name text COLLATE pg_catalog."default",
    is_active boolean,
    CONSTRAINT players_pkey PRIMARY KEY (player_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.players
    OWNER to postgres;