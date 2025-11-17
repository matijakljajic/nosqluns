// Q01: 10 reprezentativaca nekih država
MATCH (a:Athlete)-[:REPRESENTS]->(c:Country)
RETURN a.athlete_code AS athlete_id,
       a.name AS athlete,
       c.name AS country
LIMIT 10;

// Q02: Distribucija koliko se takmiči muškaraca, a koliko žena
MATCH (a:Athlete)
WITH a.gender AS gender, count(*) AS total
RETURN gender, total
ORDER BY total DESC;

// Q03: Broj disciplina po svakom sportu
MATCH (s:Sport)<-[:PART_OF]-(e:Event)
RETURN s.name AS sport,
       count(e) AS event_count
ORDER BY event_count DESC, sport;

// Q04: Mesta gde se odvija neki sport
MATCH (s:Sport)-[:USE_VENUE]->(v:Venue)
RETURN s.name AS sport,
       collect(v.name) AS venues;

// Q05: Broj sesija za Atletiku grupisane po danu kad se odvijaju
MATCH (s:Session {sport_code: 'ATH'})
WITH s.day AS day, count(*) AS sessions
RETURN day, sessions
ORDER BY day;

// Q06: 10 timova iz nekih država
MATCH (t:Team)-[:REPRESENTS]->(c:Country)
RETURN t.team_code AS team,
       t.name AS team_name,
       c.name AS country
LIMIT 10;

// Q07: Vrste zvaničnika i njihove brojke
MATCH (o:Official)
RETURN o.function AS function,
       count(*) AS officials
ORDER BY officials DESC;

// Q08: Broj trenera po državi
MATCH (co:Coach)-[:REPRESENTS]->(c:Country)
RETURN c.name AS country,
       count(co) AS coaches
ORDER BY coaches DESC
LIMIT 10;

// Q09: Broj medalja po državi
MATCH (c:Country)<-[:REPRESENTS]-(:Athlete)-[wm:WON_MEDAL]->(e:Event)
RETURN c.name AS country,
       count(DISTINCT e.event_code+wm.medal_type) AS medals
ORDER BY medals DESC
LIMIT 10;

// Q10: Ženske discipline koje nemaju reč žena u imenu
MATCH (e:Event)
WHERE e.gender = "W"
    AND NOT e.name CONTAINS "Women"
RETURN e.name AS event_name,
       e.sport_name AS sport;

// Q11: Broj sportista po timu
MATCH (t:Team)<-[:MEMBER_OF]-(a:Athlete)
RETURN t.country_code + " " + t.discipline + coalesce(" " + t.events, "") AS team,
       count(a) AS athletes
ORDER BY athletes DESC;

// Q12: Timski događaji koliko imaju timova i primeri timova
MATCH (t:Team)-[:COMPETED_IN]->(e:Event)
WITH e, collect(DISTINCT t.name)[0..5] AS sample, count(DISTINCT t) AS teams
RETURN e.sport_name + " " + e.name AS event,
       teams,
       sample AS example
ORDER BY teams DESC;

// Q13: Raspored dodele medalja u Bersi Areni
MATCH (s:Session {medal_session: true})-[:HELD_AT]->(v:Venue {name: 'Bercy Arena'})
RETURN s.event_name + " " + s.sport AS event,
       s.day AS day;

// Q14: Mesta gde se održava više sportova
MATCH (v:Venue)<-[:USE_VENUE]-(s:Sport)
WITH v, collect(s.name) AS sports
WHERE size(sports) > 2
RETURN v.name AS venue,
       sports,
       size(sports) AS sport_count;

// Q15: Zemlje sa preko 100 sportista
MATCH (c:Country)<-[:REPRESENTS]-(a:Athlete)
WITH c, count(a) AS athletes
WHERE athletes > 100
RETURN c.name AS country, athletes
ORDER BY athletes ASC;

// Q16: Broj sportista iz Srbije rođenih nakon mene
MATCH (a:Athlete)
WHERE a.birth_date >= date('2003-02-28')
    AND a.nationality_code = "SRB"
WITH a.nationality_code AS nationality, count(*) AS total
RETURN nationality, total;

// Q17: Prosečan broj zakazanih stvari po mestu događaja
MATCH (v:Venue)<-[:HELD_AT]-(s:Session)
WITH v, count(s) AS sessions
RETURN toInteger(avg(sessions)) AS avg_sessions_per_venue;

// Q18: Zemlje koje imaju sportiste reprezentativce, ali ne i trenere
MATCH (c:Country)
WHERE EXISTS { MATCH (c)<-[:REPRESENTS]-(:Athlete) }
  AND NOT EXISTS { MATCH (c)<-[:REPRESENTS]-(:Coach) }
RETURN c.name AS country;

// Q19: 100 timova koji nemaju trenera
MATCH (t:Team)
WHERE NOT EXISTS { MATCH (:Coach)-[:COACHES]->(t) }
RETURN t.country_code + " " + t.discipline + coalesce(" " + t.events, "") AS team
LIMIT 100;

// Q20: Sve sudije za neku disciplinu prikazano kao par {sudija, disciplina}
MATCH (o:Official)
WHERE o.function = "Referee"
UNWIND o.disciplines AS discipline
RETURN o.name AS official, discipline
ORDER BY discipline ASC;
