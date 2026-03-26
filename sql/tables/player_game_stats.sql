-- Table: public.player_game_stats

-- DROP TABLE IF EXISTS public.player_game_stats;

CREATE TABLE IF NOT EXISTS public.player_game_stats
(
    game_id character varying COLLATE pg_catalog."default" NOT NULL,
    team_id integer,
    player_id integer NOT NULL,
    first_name text COLLATE pg_catalog."default",
    last_name text COLLATE pg_catalog."default",
    team_city text COLLATE pg_catalog."default",
    team_name text COLLATE pg_catalog."default",
    team_abbreviation text COLLATE pg_catalog."default",
    minutes text COLLATE pg_catalog."default",
    start_position text COLLATE pg_catalog."default",
    comment text COLLATE pg_catalog."default",
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
    CONSTRAINT player_game_stats_pkey PRIMARY KEY (game_id, player_id),
    CONSTRAINT fk_pgs_game FOREIGN KEY (game_id)
        REFERENCES public.games (game_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_pgs_player FOREIGN KEY (player_id)
        REFERENCES public.players (player_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_pgs_team FOREIGN KEY (team_id)
        REFERENCES public.teams (team_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.player_game_stats
    OWNER to postgres;