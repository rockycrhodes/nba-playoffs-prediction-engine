-- Table: public.games

-- DROP TABLE IF EXISTS public.games;

CREATE TABLE IF NOT EXISTS public.games
(
    game_id character varying COLLATE pg_catalog."default" NOT NULL,
    season character varying COLLATE pg_catalog."default",
    game_date date,
    home_team_id integer,
    away_team_id integer,
    home_points integer,
    away_points integer,
    winner integer,
    play_in_flag boolean,
    playoff_flag boolean,
    season_type text COLLATE pg_catalog."default",
    CONSTRAINT games_pkey PRIMARY KEY (game_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.games
    OWNER to postgres;