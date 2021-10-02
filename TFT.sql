CREATE TABLE "Users" (
  "id" SERIAL PRIMARY KEY,
  "summonerName" varchar,
  "summonerId" varchar,
  "rankedLeague" varchar,
  "puuid" varchar,
  "leaguepoints" int,
  "region" varchar
);

CREATE TABLE "Match" (
  "id" SERIAL PRIMARY KEY,
  "matchId" varchar,
  "date" timestamp,
  "uploadedAt" timestamp,
  "region" varchar
);

CREATE TABLE "MatchInfo" (
  "id" SERIAL PRIMARY KEY,
  "matchId" int,
  "matchData" varchar,
  "userId" int,
  "region" varchar,
  "placement" int
);

CREATE TABLE "MatchTraits" (
  "matchInfoId" int,
  "traitName" varchar,
  "numUnits" int,
  "currentTier" int
);

CREATE TABLE "MatchUnits" (
  "matchInfoId" int,
  "UnitName" varchar,
  "unitStar" int,
  "item1" int,
  "item2" int,
  "item3" int
);

ALTER TABLE "MatchInfo" ADD FOREIGN KEY ("matchId") REFERENCES "Match" ("id");

ALTER TABLE "MatchInfo" ADD FOREIGN KEY ("userId") REFERENCES "Users" ("id");

ALTER TABLE "MatchTraits" ADD FOREIGN KEY ("matchInfoId") REFERENCES "MatchInfo" ("id");

ALTER TABLE "MatchUnits" ADD FOREIGN KEY ("matchInfoId") REFERENCES "MatchInfo" ("id");
