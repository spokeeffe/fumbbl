# FUMBBL League Admin Tool

# Goal

Deliver a website that allows league managers to calculate standings, player stats and achievements for their leagues.

# Resources

FUMBBL API docs: [https://fumbbl.com/apidoc/](https://fumbbl.com/apidoc/)  
FUMBBL API JSON: [https://fumbbl.com/apidoc.json](https://fumbbl.com/apidoc.json)

# Context

FUMBBL is a website for playing fantasy football games (similar to Blood Bowl). On the website you can also run player managed leagues.

# Discrete elements

* Web UI (hosted on Vercel)  
  * Should be simple and clean  
* Ability to input league information. A league is uniquely identified by a Group ID  
  * Group ID (INTEGER)  
  * Ruleset ID (INTEGER)  
  * League Name (pulled via Group ID)  
  * Tiebreakers (default to: Head to Head, touchdowns delta, casualties delta, coin toss)  
* League Data Retrieval  
  * Given a selected league, use the API to pull all tournaments for that league and display them in descending order.  
  * For each tournament it should display: season #, tournament name  
  * User should be able to select to only display tournaments for a specific season  
* Standings  
  * Ability to retrieve matches from selected tournaments for a league  
  * Compute standings based on league tiebreakers  
  * Output standings in a csv format  
  * Check for 4-0 victories  
* Player Stats  
  * Ability to retrieve player SPP from start to end of tournaments for a league  
  * Ability to retrieve player stats from selected tournaments for a league  
    * Stats include: completions, touchdowns, casualties, interceptions, fouls  
  * Ability to retrieve player status; active, dead or retired
  * Player stats flow:
    1. For each selected tournament, select all the teams that participated in that tournament (exlcuding Filler teams that lost all their games)
    2. Find all the matches that occurred for the selected teams in the selected tournaments
    3. For each match at a player level pull the following:
      a. tournament_id
      b. match_id
      c. player_id
      d. player_name
      e. team_id
      f. team_name
      g. count of star player points earned (spp)
      h. count of touchdowns
      i. count of completions
      i. count of casualties
      k. count of fouls
      l. count of interceptions
      m. count of blocks
      n. sum of rushing yards
      o. sum of passing yards
    4. Enrich the player level data with:
      a. Larson = 1 if the player had at least 1 touchdown, 1 casualty, 1 completion, and 1 interception ALL IN THE SAME MATCH, else = 0
      b. Mean Scoring Machine = 1 if the player scored 3 or more touchdowns in the same match
      c. Triple X = 1 if the player had 3 or more casualties in the same match
      d. Aerydynamic Aim = 1 if the player had 4 or more completions in the same match
    5. Aggregate the match and player level data to just the player level, summing the spp, touchdowns, completions, casualties, fouls, interceptions, blocks, rushing yards, passing yards, larson, mean scoring machine, triple x, and aerodynamic aim columns
    6. Enrich the aggregate data with the following columns:
      a. Games Played = count of games the player participated in
      c. Blocking Scorer = min(touchdowns, casualties) where each input must be > 0
      d. Blocking Thrower = min(completions, casualties) where each input must be > 0
      e. Scoring Thrower = min(touchdowns, completions) where each input must be > 0
      f. Triple = min (touchdowns, casualties, completions) where each input must be > 0
      g. All Rounder = min (touchdowns, casualties, completions, interceptions) where each input must be > 0
      k. Player Status = dead, retired, or active (players current status)
    7. Final output format should be:
      * Player Name (STRING)
      * Team Name (STRING)
      * Player Status (STRING)
      * Games Played (INT)
      * SPP (INT)
      * Completions (INT)
      * Touchdowns (INT)
      * Casualties (INT)
      * Fouls (INT)
      * Interceptions (INT)
      * Blocks (INT)
      * Rushing Yards (INT)
      * Passing Yards (INT)
      * Scoring Thrower (INT)
      * Blocking Thrower (INT)
      * Blocking Scorer (INT)
      * Triple (INT)
      * All Rounder (INT)
      * Larson (INT)
      * Mean Scoring Machine (INT)
      * Triple X (INT)
      * Aerodynamic Aim (INT)
  * For each tournament this player data should be visible in a very similar fashion to the Standings at a tournament level, with the collapsible headers
* Player Achievements  
  * From the stats be able to define bespoke achievements  
    * Tournament achievements
      * LOGIC
        * Each achievment is only awarded to the player with the highest value for the category
        * If multiple players are tied then no achievement is given
        * Dead players are ineligible to receive achievements and should be excluded
      * The unique achievements are:
        * SPP
        * Completions
        * Touchdowns
        * Casualties
        * Fouls
        * Interceptions
        * scoring thrower
        * blocking scorer
        * blocking thrower
        * triple
    * Player achievements - these can be earned in any season when a player meets the criteria. A player can earn multiple of these
      * Triple X - inflicting 3 or more casualties in a single game
      * Mean Scoring Machine - scoring 3 or more touchdowns in a single game
      * Aerodynamic Aim - having 4 or more completions in a single game
      * Larson - having at least 1 touchdown, 1 casualty, 1 completion and 1 interception in a single game
    * SPP thresholds - each player can only achieve these once in their career for passing specific SPP thresholds
     * Star - reaching 51 or more SPP
     * Super Star - reaching 76 or more SPP
     * Mega Star - reaching 126 or more SPP
     * Legend - reaching 176 or more SPP
  * When we generate achievements we need a table for each tournament that lists the following:
    * Tournament
    * Achievement Name
    * Player Name
    * Team Name
    * Match URL (this should hyperlink to FUMBBL and follow this format: https://fumbbl.com/p/match?id=[match_id])
* Safety  
  * The script should rate limit API calls to avoid abuse  
  * API failures should be handled gracefully and the user alerted  
  * Inputs should be tightly controlled/typed and validated to prevent prompt or SQL injection