-- V1__init.sql: initial inventory schema for kotlin_ad_server

CREATE TABLE advertisers (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    domain          TEXT NOT NULL UNIQUE
);

CREATE TABLE campaigns (
    id              TEXT PRIMARY KEY,
    advertiser_id   TEXT NOT NULL REFERENCES advertisers(id),
    category        TEXT NOT NULL,             -- IAB primary category
    bid_price       NUMERIC(10, 4) NOT NULL,   -- USD CPM
    frequency_cap   INTEGER NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_campaigns_active ON campaigns (active) WHERE active = TRUE;

CREATE TABLE creatives (
    id              TEXT PRIMARY KEY,
    campaign_id     TEXT NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    width           INTEGER NOT NULL,
    height          INTEGER NOT NULL,
    markup          TEXT NOT NULL
);

CREATE INDEX idx_creatives_campaign ON creatives (campaign_id);
