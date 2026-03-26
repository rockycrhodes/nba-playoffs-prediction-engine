-- Table: public.teams

-- DROP TABLE IF EXISTS public.teams;

CREATE TABLE IF NOT EXISTS public.teams
(
    team_id integer NOT NULL,
    team_name text COLLATE pg_catalog."default",
    team_abbreviation text COLLATE pg_catalog."default",
    team_nickname text COLLATE pg_catalog."default",
    team_city text COLLATE pg_catalog."default",
    team_state text COLLATE pg_catalog."default",
    team_year_founded integer,
    CONSTRAINT teams_pkey PRIMARY KEY (team_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.teams
    OWNER to postgres;