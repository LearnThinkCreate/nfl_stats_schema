# NFL Statistics Database Project

## Database Schema and Setup

This section details the architecture, design principles, and setup process for the PostgreSQL database powering the NFL Statistics project. The schema is meticulously designed to support efficient storage, querying, and analysis of comprehensive NFL play-by-play and player data, showcasing expertise relevant to Data Engineer and Data Analyst roles.

### Overview

The database employs **PostgreSQL** and is structured around dimensional modeling principles, providing a robust foundation for analytical workloads. It houses detailed information on players, teams, games, and granular play-by-play statistics spanning multiple NFL seasons.

### Key Design Principles & Highlights

The schema design prioritizes performance, scalability, and analytical flexibility:

1.  **Dimensional Modeling:** A clear distinction is maintained between:
    *   **Dimension Tables:** `players`, `teams`, `games` store descriptive attributes.
    *   **Fact Tables:** `plays` (play-by-play), `playstats` (player-specific stats per play), `snap_counts` store quantitative measures and events, linking back to dimensions.

2.  **Efficient Partitioning:**
    *   The largest fact tables (`plays`, `playstats`) are **partitioned by `season`** using PostgreSQL's declarative partitioning (RANGE).
    *   **Benefit:** This significantly boosts query performance for season-specific analyses (a common use case) by allowing the query planner to scan only relevant partitions (partition pruning). It also simplifies data management (e.g., archiving or dropping old seasons).
    *   Maintenance functions (`create_season_partition`, `create_playstats_partition`) are provided for seamless addition of new season partitions.

3.  **Strategic Use of Materialized Views:**
    *   `player_game_stats`: A foundational **materialized view** aggregates granular play-level data (`plays`, `playstats`) into pre-computed game-level statistics for each player. This drastically speeds up queries for game, season, and career summaries.
    *   `player_relevance`: An intelligent **materialized view** calculates a composite relevance score for each player based on weighted career and recent performance metrics. This enables highly efficient player sorting, searching, and ranking functionalities within applications using this database.

4.  **Layered Aggregation via Views:**
    *   Building upon the `player_game_stats` materialized view, standard SQL **views** provide progressively higher levels of aggregation:
        *   `player_season_stats`: Aggregates game stats to the season level.
        *   `player_career_stats`: Aggregates season stats to the career level.
    *   A `_with_metrics` view pattern is used at each aggregation level (e.g., `player_game_stats_with_metrics`) to calculate derived metrics (rates, percentages, EPA, shares, WOPR) separately, keeping the base aggregation logic clean and performant.

5.  **Comprehensive Indexing Strategy:**
    *   Indexes are strategically applied throughout the schema to optimize various query patterns:
        *   **Primary & Foreign Keys:** Ensure data integrity and join performance.
        *   **Common Filters:** Indexes on `season`, `week`, `team_abbr`, `player_id`, `position`, `game_type`.
        *   **Fuzzy Text Search:** GIN indexes using `pg_trgm` on `players.display_name`, `teams.team_name`, and concatenated player names in `plays` enable efficient partial and misspelled name searches.
        *   **Partial Indexes:** Used effectively to index subsets of data (e.g., only 'Active' players, specific `play_type` values), minimizing index size and maximizing utility for targeted queries.
        *   **Covering Indexes (`INCLUDE`):** Implemented where appropriate (e.g., on `plays`, `player_game_stats`) to enable index-only scans for certain queries, avoiding table heap access.
        *   **Situational Flag Indexes:** Indexes on flags like `is_leading`, `is_red_zone`, `is_late_down` in `plays` and `playstats` accelerate filtering based on game context.
        *   **Collation Support:** Demonstrated awareness of locale-specific sorting/comparison (`idx_players_name_collation`).

6.  **Modularity and Maintainability:**
    *   The schema is defined across logically separated and numbered SQL files ( `01_...` to `13_...`), facilitating understanding and sequential setup.
    *   PL/pgSQL functions (`get_filtered_season_stats`, partition creation functions) encapsulate logic, enhancing reusability and maintainability.

### Schema Components

*   **Dimensions:** `teams`, `players`, `games`
*   **Facts:** `plays` (partitioned), `playstats` (partitioned), `snap_counts`
*   **Materialized Views:** `player_game_stats`, `player_relevance`
*   **Standard Views:** `player_season_stats`, `player_career_stats`, `player_game_stats_with_metrics`, `player_season_stats_with_metrics`, `player_career_stats_with_metrics`, `player_career_team`
*   **Functions:** `get_filtered_season_stats`, `create_season_partition`, `create_playstats_partition`
*   **Extensions:** `pg_trgm`

### Setup Instructions

1.  **Prerequisites:** Ensure PostgreSQL is installed and running. The `pg_trgm` extension is required.
2.  **Initialization:** Execute the provided SQL files in numerical order (from `01_extensions.sql` to `13_func_get_filtered_stats.sql`) against your target database. A `setup_database.sh` script is typically used to automate this sequence:
    ```bash
    # Example (replace with actual credentials/database name)
    # PGPASSWORD=yourpassword psql -h localhost -U youruser -d nfl_stats -f 01_extensions.sql
    # PGPASSWORD=yourpassword psql -h localhost -U youruser -d nfl_stats -f 02_teams.sql
    # ... and so on for all 13 files
    # Or run the setup script:
    # ./setup_database.sh <db_user> <db_name> <db_host>
    ```
3.  **Materialized View Refresh:** After initial data loading, the materialized views (`player_game_stats`, `player_relevance`) will need to be refreshed periodically to reflect new data:
    ```sql
    REFRESH MATERIALIZED VIEW player_game_stats;
    REFRESH MATERIALIZED VIEW player_relevance;
    ```

### Demonstrated Skills

This database schema demonstrates proficiency in:

*   Relational Database Design & Data Modeling (Dimensional Modeling)
*   Advanced SQL and PostgreSQL Features (Partitioning, Materialized Views, Window Functions, CTEs, PL/pgSQL)
*   Database Performance Tuning & Optimization
*   Strategic Indexing (B-Tree, GIN, Partial, Covering Indexes)
*   Schema Management and Maintainability
*   Data Warehousing Concepts