// Q01: Podijumi po sportu sa gledišta država
MATCH (sport:Sport)
CALL {
  WITH sport
  MATCH (sport)<-[:PART_OF]-(e:Event)<-[medal:WON_MEDAL]-(entity)
  MATCH (entity)-[:REPRESENTS]->(country:Country)
  RETURN country.name AS country, count(medal) AS medals
  ORDER BY medals DESC
  LIMIT 3
}
WITH sport, country, medals
RETURN sport.name AS sport,
       collect({country: country, medals: medals}) AS podium
ORDER BY sport.name;

// Q02: Sportisti više disciplina koji su osvojili medalje
MATCH (a:Athlete)
WITH a, a.disciplines AS disciplines
WHERE size(disciplines) > 1
OPTIONAL MATCH (a)-[m:WON_MEDAL]->(e:Event)
WITH a, disciplines,
     [info IN collect({discipline: e.sport_name, event: e.name, medal: m.medal_type}) WHERE info.event IS NOT NULL] AS medals
WHERE size(medals) <> 0
RETURN a.name AS athlete,
       disciplines,
       medals
ORDER BY size(disciplines) DESC, size(medals) DESC;

// Q03: Koliko medalja ima po sportisti grupisano po zemljama
MATCH (c:Country)
OPTIONAL MATCH (a:Athlete)-[:REPRESENTS]->(c)
WITH c, count(DISTINCT a) AS athlete_count
OPTIONAL MATCH (c)<-[:REPRESENTS]-(entity)-[m:WON_MEDAL]->(:Event)
WITH c, athlete_count, count(m) AS medals, count(DISTINCT entity) AS medalists
WITH c,
     athlete_count,
     medals,
     medalists,
     CASE
       WHEN athlete_count = 0 THEN NULL
       ELSE round(toFloat(medals) / athlete_count, 2)
     END AS medals_per_athlete
WHERE medals_per_athlete IS NOT NULL
RETURN c.name AS country,
       athlete_count,
       medals_per_athlete
ORDER BY medals_per_athlete DESC
LIMIT 15;

// Q04: Koliko je bilo ukupno događaja kategorisano po mestima, a koliko je ukupno bilo dodela medalja
MATCH (v:Venue)<-[:HELD_AT]-(s:Session)
WITH v,
     count(s) AS total_sessions,
     sum(CASE WHEN s.medal_session THEN 1 ELSE 0 END) AS medal_sessions
RETURN v.name AS venue,
       total_sessions,
       medal_sessions
ORDER BY medal_sessions DESC, total_sessions DESC
LIMIT 10;

// Q05: Trener i broj medalja koji je osvojio njegov/njen tim
MATCH (coach:Coach)-[:COACHES]->(t:Team)
OPTIONAL MATCH (t)<-[:MEMBER_OF]-(a:Athlete)-[medal:WON_MEDAL]->(e:Event)
WITH coach, t,
     [info IN collect({event: e.name, medal: medal.medal_type}) WHERE info.event IS NOT NULL] AS medal_events
WHERE size(medal_events) <> 0
RETURN coach.name AS coach,
       t.name AS team,
       size(medal_events) AS medal_event_count
ORDER BY medal_event_count DESC, coach.name;

// Q06: Istorijat takmičenja 20 pobednika
MATCH (a:Athlete)-[win:WON_MEDAL {medal_type: "Gold Medal"}]->(e:Event)
MATCH (a)-[r:COMPETED_IN]->(e)
WITH a, e, win, r
ORDER BY r.stage_datetime ASC
WITH a, e, win,
     collect({
       stage: r.event_stage,
       result: CASE
                 WHEN r.result IS NOT NULL THEN r.result
                 ELSE 'N/A'
               END,
       rank:   CASE
                 WHEN r.rank IS NOT NULL THEN r.rank
                 ELSE 'N/A'
               END,
       at: r.stage_datetime
     }) AS journey
RETURN a.name AS athlete,
       e.sport_name AS event,
       win.medal_type AS medal,
       journey
ORDER BY athlete, event
LIMIT 20;

// Q07: Gustina rasporeda po sportu
MATCH (sport:Sport)
MATCH (session:Session {sport_code: sport.sport_code})
WITH sport,
     count(session) AS sessions,
     count(DISTINCT session.day) AS days
WHERE days > 0
RETURN sport.name AS sport,
       sessions,
       days,
       round(toFloat(sessions) / days, 2) AS sessions_per_day
ORDER BY sessions_per_day DESC;

// Q08: Preformanse timova
MATCH (t:Team)-[r:COMPETED_IN]->(:Event)
WITH t,
     count(r) AS appearances,
     round(avg(r.rank), 2) AS avg_rank
RETURN t.name AS team_name,
       appearances,
       avg_rank
ORDER BY appearances DESC, avg_rank DESC
LIMIT 15;
