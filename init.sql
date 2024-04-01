CREATE SCHEMA IF NOT EXISTS nrod;

CREATE TABLE IF NOT EXISTS nrod.reference_smart (
    td              text,
    from_berth      text,
    to_berth        text,
    from_line       text,
    to_line         text,
    berth_offset    integer,
    platform        text,
    event           text,
    route           text,
    stanox          text,
    stanme          text,
    step_type       text,
    comment         text
);

CREATE TABLE IF NOT EXISTS nrod.reference_corpus (
    stanox          integer,
    uic_code        text,
    location_code   text,
    tiploc_code     text,
    nlc             integer,
    nlc_description text,
    description     text
);
