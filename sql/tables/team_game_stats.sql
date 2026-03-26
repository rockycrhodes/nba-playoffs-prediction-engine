-- Table: public.team_game_stats

-- DROP TABLE IF EXISTS public.team_game_stats;

CREATE TABLE IF NOT EXISTS public.team_game_stats
(
    game_id character varying COLLATE pg_catalog."default" NOT NULL,
    team_id integer NOT NULL,
    team_city text COLLATE pg_catalog."default",
    team_name text COLLATE pg_catalog."default",
    team_abbreviation text COLLATE pg_catalog."default",
    minutes text COLLATE pg_catalog."default",
    fgm integer,
    fga integer,
    fg_pct numeric,
    fg3m integer,
    fg3a integer,
    fg3_pct numeric,
    ftm integer,
    fta integer,
    ft_pct numeric,
    oreb integer,
    dreb integer,
    reb integer,
    ast integer,
    stl integer,
    blk integer,
    turnovers integer,
    pf integer,
    pts integer,
    plus_minus numeric,
    season character varying COLLATE pg_catalog."default",
    season_type text COLLATE pg_catalog."default",
    CONSTRAINT team_game_stats_pkey PRIMARY KEY (game_id, team_id),
    CONSTRAINT fk_tgs_game FOREIGN KEY (game_id)
        REFERENCES public.games (game_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_tgs_team FOREIGN KEY (team_id)
        REFERENCES public.teams (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.team_game_stats
    OWNER to postgres;