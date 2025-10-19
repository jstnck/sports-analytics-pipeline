# ESPN NBA API Reference

This document provides a comprehensive guide to ESPN's NBA API endpoints based on live API exploration and real data structures observed during development of our sports analytics pipeline.

## Base Information

**Base URL**: `https://site.api.espn.com/apis/site/v2/sports/basketball/nba`

**Rate Limiting**: No official rate limits published, but we use conservative delays (0.2s default) to be respectful
- Our pipeline uses 0.05s minimum delay, 2.0s maximum delay
- Burst handling: 200 requests with 0.5s cooldown

**Authentication**: None required for public endpoints

---

## Core Endpoints

### 1. Scoreboard Endpoint

**URL**: `{BASE_URL}/scoreboard`

**Parameters**:
- `dates` (required): Date in YYYYMMDD format (e.g., `20241025`)

**Purpose**: Get schedule and basic game information for a specific date, including scores for completed games.

#### Top-Level Structure

```
Response Root
├── leagues[]           # League information (NBA metadata)
├── events[]           # Array of games/events for the date
├── day                # Date information
├── week               # Week information  
└── season             # Season metadata
```

#### Event Structure

Each event in the `events[]` array represents a single NBA game:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `id` | string | Unique ESPN event identifier | `"401704643"` |
| `uid` | string | Universal identifier | `"s:40~l:46~e:401704643"` |
| `date` | string | ISO 8601 timestamp | `"2024-10-25T23:00Z"` |
| `name` | string | Full game description | `"Brooklyn Nets at Orlando Magic"` |
| `shortName` | string | Abbreviated game description | `"BKN @ ORL"` |
| `status` | object | Game status and timing | See Status Structure below |
| `competitions[]` | array | Competition details | See Competition Structure below |
| `season` | object | Season information | Year, type, etc. |
| `links[]` | array | Related ESPN URLs | Gamecast, recap, etc. |

#### Competition Structure

The `competitions[0]` object contains the core game data:

```
competitions[0]
├── id                 # Competition ID
├── date              # Game date/time
├── startDate         # Scheduled start time  
├── timeValid         # Whether timing is accurate
├── neutralSite       # Boolean - neutral venue?
├── conferenceCompetition # Boolean - conference game?
├── playByPlayAvailable   # Boolean - PBP data available?
├── competitors[]     # Array of 2 teams (home/away)
├── venue            # Venue information
├── status           # Detailed game status
├── broadcasts[]     # TV/streaming info
├── notes[]          # Game notes
├── headlines[]      # Related headlines
└── format           # Competition format details
```

#### Competitor Structure

Each team in `competitors[]` array:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `id` | string | Team ESPN ID | `"10"` |
| `uid` | string | Universal team ID | `"s:40~l:46~t:10"` |
| `type` | string | Always "team" | `"team"` |
| `order` | number | Display order (0 or 1) | `0` |
| `homeAway` | string | "home" or "away" | `"home"` |
| `winner` | boolean | Did this team win? | `true` |
| `score` | string | Final/current score | `"116"` |
| `team` | object | Team details | See Team Structure |
| `statistics[]` | array | Team stats (if available) | FG%, rebounds, etc. |
| `records[]` | array | Team records | Overall, conference, etc. |
| `leaders[]` | array | Statistical leaders | Points, rebounds, assists |
| `linescores[]` | array | Quarter-by-quarter scores | Q1, Q2, Q3, Q4 |

#### Team Structure

Within each competitor's `team` object:

```
team
├── id               # ESPN team ID
├── uid              # Universal identifier  
├── displayName      # Full team name ("Orlando Magic")
├── name             # Team name ("Magic")
├── location         # City ("Orlando")
├── abbreviation     # 3-letter code ("ORL")
├── shortDisplayName # Short name ("Magic")
├── color           # Primary hex color
├── alternateColor  # Secondary hex color
├── isActive        # Boolean - currently active?
├── logo            # Logo URL
├── links[]         # Team-related URLs
└── venue           # Home venue information
```

#### Venue Structure

Venue information in `competitions[0].venue`:

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `id` | string | Venue ESPN ID | `"194"` |
| `fullName` | string | Complete venue name | `"Kia Center"` |
| `address` | object | Venue address | City, state details |
| `indoor` | boolean | Indoor venue? | `true` |

#### Status Structure

Game status in both `event.status` and `competition.status`:

```
status
├── clock           # Time remaining (seconds or 0.0 if final)
├── displayClock    # Human readable time ("0.0", "5:23")
├── period          # Quarter/period number (1-4, 5+ for OT)
├── type
│   ├── id          # Status ID ("1"=scheduled, "2"=live, "3"=final)
│   ├── name        # Status name ("STATUS_FINAL", "STATUS_IN_PROGRESS")
│   ├── state       # Game state ("pre", "in", "post")
│   ├── completed   # Boolean - game finished?
│   ├── description # Human description ("Final", "2nd Quarter")
│   ├── detail      # More detail ("Final", "5:23 - 2nd Quarter")
│   └── shortDetail # Short version ("Final", "5:23 2Q")
```

Common status values:
- **Pre-game**: `id: "1"`, `state: "pre"`, `name: "STATUS_SCHEDULED"`
- **Live**: `id: "2"`, `state: "in"`, `name: "STATUS_IN_PROGRESS"`  
- **Final**: `id: "3"`, `state: "post"`, `name: "STATUS_FINAL"`

---

### 2. Summary Endpoint

**URL**: `{BASE_URL}/summary`

**Parameters**:
- `event` (required): ESPN event ID from scoreboard endpoint

**Purpose**: Get detailed game information including full box scores, play-by-play, and advanced statistics.

#### Top-Level Structure

```
Response Root
├── header              # Basic game info (similar to scoreboard)
├── boxscore           # Detailed player and team statistics
├── gameInfo           # Game details (officials, attendance, etc.)
├── leaders            # Statistical leaders for this game
├── plays              # Play-by-play data (if available)
├── winprobability     # Win probability chart data
├── article            # Related articles
├── news               # News items
├── videos             # Video highlights
├── broadcasts         # Broadcast information
├── odds               # Betting odds (if available)
├── injuries           # Injury reports
├── standings          # Current standings
├── seasonseries       # Head-to-head season series
└── meta               # Metadata about the response
```

#### Boxscore Structure

The `boxscore` object contains detailed statistics:

```
boxscore
├── teams[]            # Team-level statistics (2 teams)
└── players[]          # Player-level statistics (2 team rosters)
```

#### Team Statistics

Each team in `boxscore.teams[]`:

| Field | Type | Description |
|-------|------|-------------|
| `team` | object | Team identifier and basic info |
| `homeAway` | string | "home" or "away" |
| `displayOrder` | number | Display order (0 or 1) |
| `statistics[]` | array | Array of statistical categories |

**Available Team Statistics**: 
- `fieldGoalsMade-fieldGoalsAttempted` (e.g., "45-89")
- `fieldGoalPct` (e.g., "50.6")  
- `threePointFieldGoalsMade-threePointFieldGoalsAttempted`
- `threePointFieldGoalPct`
- `freeThrowsMade-freeThrowsAttempted`
- `freeThrowPct`
- `totalRebounds`, `offensiveRebounds`, `defensiveRebounds`
- `assists`, `steals`, `blocks`, `turnovers`
- `teamTurnovers`, `totalTurnovers`
- `technicalFouls`, `totalTechnicalFouls`, `flagrantFouls`
- `turnoverPoints`, `fastBreakPoints`, `pointsInPaint`
- `fouls`, `largestLead`

#### Player Statistics

Each team in `boxscore.players[]`:

```
players[0]  # Home team
├── team            # Team reference
├── displayOrder    # Display order
└── statistics[]    # Array of stat categories (usually 1 main category)
    └── [0]         # Main statistics
        ├── athletes[]     # Player data and stats
        ├── keys[]        # Stat column keys  
        ├── labels[]      # Human-readable headers
        ├── names[]       # Detailed names
        ├── descriptions[]# Stat descriptions
        └── totals[]      # Team totals row
```

**Player Stat Labels**: `["MIN", "FG", "3PT", "FT", "OREB", "DREB", "REB", "AST", "STL", "BLK", "TO", "PF", "+/-", "PTS"]`

Each player in the `athletes[]` array contains:
- Basic player info (id, displayName, jersey, position)
- Statistics array corresponding to the labels above
- Links to player profile and headshot

#### Game Info Structure

The `gameInfo` object provides additional context:

```
gameInfo
├── venue              # Detailed venue information
├── attendance         # Attendance figure  
├── officials[]        # Game officials/referees
├── temperature        # Game temperature (outdoor games)
├── weather           # Weather conditions (outdoor games)
└── gameNote          # Special notes about the game
```

---

## Data Patterns and Best Practices

### Date Handling
- ESPN uses UTC timestamps in ISO 8601 format
- Scoreboard endpoint uses YYYYMMDD format for date parameter
- Game times are in UTC, convert to local timezone as needed

### ID Relationships
- Event IDs from scoreboard are used as `event` parameter in summary
- Team IDs are consistent across endpoints
- UIDs follow pattern: `s:40~l:46~t:{team_id}` or `s:40~l:46~e:{event_id}`

### Data Availability Timing
- **Scoreboard**: Available immediately when games are scheduled
- **Summary basic**: Available when games are scheduled  
- **Summary boxscore**: Populated during/after games
- **Play-by-play**: Available during live games and after completion

### Handling Different Game States

#### Pre-Game
- `status.type.state = "pre"`
- Limited data available
- No scores or detailed statistics
- Lineup information may be available in summary

#### Live Game
- `status.type.state = "in"`
- Scores and statistics update in real-time
- Quarter/period and clock information available
- Play-by-play data streams live

#### Post-Game
- `status.type.state = "post"`
- Final scores and complete statistics
- Full play-by-play available
- Additional analysis and highlights

### Error Handling

Common error scenarios:
- **Invalid date**: Returns empty events array
- **Invalid event ID**: Returns error response
- **Future dates**: May return scheduled games with limited data
- **Off-season dates**: Returns empty events array

### Rate Limiting Best Practices

Based on our pipeline experience:
1. Use minimum 0.05s delays between requests
2. Implement burst protection (200 requests, then 0.5s cooldown)
3. Cache responses when possible - data doesn't change frequently for completed games
4. Be respectful - ESPN provides this data for free

---

## Example Queries

### Get today's games
```bash
curl "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=$(date +%Y%m%d)"
```

### Get specific game details
```bash
# First get event ID from scoreboard
EVENT_ID=$(curl -s "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=20241025" | jq -r '.events[0].id')

# Then get full game summary
curl "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event=${EVENT_ID}"
```

### Extract specific data
```bash
# Get all games with scores for a date
curl -s "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates=20241025" | \
  jq '.events[] | {
    id, 
    name, 
    status: .status.type.description,
    home: .competitions[0].competitors[] | select(.homeAway=="home") | {team: .team.displayName, score},
    away: .competitions[0].competitors[] | select(.homeAway=="away") | {team: .team.displayName, score}
  }'
```

---

## Additional Useful Endpoints

Beyond the core scoreboard and summary endpoints, ESPN provides several other NBA endpoints that contain valuable data for analytics and applications.

### 3. Teams Endpoint

**URL**: `{BASE_URL}/teams`

**Parameters**: None required

**Purpose**: Get comprehensive information about all NBA teams including basic details, logos, and links.

#### Structure

```
Response Root
└── sports[]
    └── [0].leagues[]
        └── [0].teams[]
            └── team
                ├── id                # ESPN team ID
                ├── uid               # Universal identifier
                ├── slug              # URL-friendly name ("atlanta-hawks")
                ├── abbreviation      # 3-letter code ("ATL")
                ├── displayName       # Full name ("Atlanta Hawks")
                ├── shortDisplayName  # Short name ("Hawks")
                ├── name              # Team name ("Hawks")
                ├── nickname          # Alternate name ("Atlanta")
                ├── location          # City ("Atlanta")
                ├── color             # Primary hex color ("c8102e")
                ├── alternateColor    # Secondary hex color ("fdb927")
                ├── isActive          # Boolean - currently active?
                ├── isAllStar         # Boolean - All-Star team?
                ├── logos[]           # Array of logo URLs (full, dark, scoreboard)
                └── links[]           # Related URLs (clubhouse, roster, stats, schedule, tickets, depth chart)
```

**Use Cases**:
- Team reference data and mapping
- Logo URLs for UI applications
- Team color schemes for visualizations
- Links to ESPN team pages

### 4. Standings Endpoint

**URL**: `https://site.api.espn.com/apis/v2/sports/basketball/nba/standings`

**Parameters**: None required (current season)

**Purpose**: Get current NBA standings with comprehensive team statistics and records.

#### Structure

```
Response Root
├── id                    # League ID
├── name                  # League name
├── abbreviation          # League abbreviation
├── seasons[]            # Season information
└── children[]           # Conference/division breakdown
    └── standings
        └── entries[]    # Team standings entries
            ├── team     # Team reference
            └── stats[]  # Statistical categories
```

**Available Statistics**:
- `wins`, `losses` - Win/loss records
- `winPercent` - Winning percentage
- `gamesBehind` - Games behind leader
- `streak` - Current win/loss streak
- `avgPointsFor`, `avgPointsAgainst` - Average points scored/allowed
- `pointDifferential` - Point differential
- `divisionWinPercent` - Division record percentage
- `leagueWinPercent` - League record percentage
- `playoffSeed` - Current playoff seeding
- `overall`, `Home`, `Road` - Record splits
- `vs. Div.`, `vs. Conf.` - Divisional/conference records
- `Last Ten Games` - Recent performance

**Example Usage**:
```bash
# Get current standings with key stats
curl -s "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings" | \
  jq '.children[0].standings.entries[] | {
    team: .team.displayName,
    wins: (.stats[] | select(.name=="wins") | .value),
    losses: (.stats[] | select(.name=="losses") | .value),
    winPct: (.stats[] | select(.name=="winPercent") | .value)
  }'
```

### 5. Team Roster Endpoint

**URL**: `{BASE_URL}/teams/{TEAM_ID}/roster`

**Parameters**:
- `TEAM_ID` (required): ESPN team ID (e.g., "1" for Atlanta Hawks)

**Purpose**: Get current roster information for a specific team including player details, contracts, and status.

#### Structure

```
Response Root
├── team              # Team reference information
├── season            # Current season information
├── status            # Roster status
├── timestamp         # Last updated timestamp
├── coach             # Head coach information
└── athletes[]        # Player roster array
    ├── id            # Player ESPN ID
    ├── uid           # Universal player identifier
    ├── displayName   # Full player name
    ├── shortName     # Abbreviated name
    ├── firstName     # First name
    ├── lastName      # Last name
    ├── fullName      # Complete full name
    ├── jersey        # Jersey number
    ├── position      # Player position
    ├── age           # Current age
    ├── height        # Height in inches
    ├── weight        # Weight in pounds
    ├── displayHeight # Formatted height ("6'8\"")
    ├── displayWeight # Formatted weight ("250 lbs")
    ├── experience    # Years of NBA experience
    ├── college       # College/university attended
    ├── birthPlace    # Birth location
    ├── dateOfBirth   # Birth date
    ├── debutYear     # NBA debut year
    ├── headshot      # Player photo URL
    ├── status        # Player status (active, injured, etc.)
    ├── injuries[]    # Current injury information
    ├── contract      # Contract details
    ├── contracts[]   # Contract history
    └── links[]       # Player-related URLs
```

**Use Cases**:
- Player reference data and photos
- Roster analysis and team composition
- Contract and salary information
- Injury tracking and status monitoring

### 6. News Endpoint

**URL**: `{BASE_URL}/news`

**Parameters**: None required

**Purpose**: Get current NBA news articles and stories from ESPN.

#### Structure

```
Response Root
├── header            # News section header
├── link              # Main news page link
└── articles[]        # News articles array
    ├── id            # Article ID
    ├── headline      # Article headline
    ├── description   # Article summary/excerpt
    ├── published     # Publication timestamp
    ├── lastModified  # Last update timestamp
    ├── byline        # Author information
    ├── type          # Article type (story, video, etc.)
    ├── premium       # Boolean - premium content?
    ├── images[]      # Article images
    ├── categories[]  # News categories/tags
    ├── links[]       # Related URLs
    └── dataSourceIdentifier # Source system
```

**Use Cases**:
- NBA news aggregation
- Content management systems
- Social media automation
- News sentiment analysis

### 7. Transactions Endpoint

**URL**: `{BASE_URL}/transactions`

**Parameters**: 
- `year` (optional): Specific year (defaults to current season)

**Purpose**: Get NBA transactions including trades, signings, releases, and other roster moves.

#### Structure

```
Response Root
├── count            # Total number of transactions
├── pageCount        # Number of pages available
├── pageIndex        # Current page index
├── pageSize         # Items per page
├── requestedYear    # Year requested
├── season           # Season information
├── status           # Response status
├── timestamp        # Response timestamp
└── transactions[]   # Transaction array
    ├── date         # Transaction date
    ├── description  # Transaction description
    └── team         # Team involved (if applicable)
```

**Transaction Types Include**:
- Player signings and releases
- Trades and acquisitions
- Injury list placements (IL, DNP)
- Contract extensions
- Draft selections
- Coaching changes

**Example Usage**:
```bash
# Get recent transactions
curl -s "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/transactions" | \
  jq '.transactions[0:5] | .[] | {date, description, team: .team.displayName}'
```

---

## Endpoint Comparison Matrix

| Endpoint | Data Type | Update Frequency | Historical Data | Use Case |
|----------|-----------|------------------|-----------------|----------|
| **Scoreboard** | Game schedules & scores | Real-time during games | Limited (current season) | Game tracking, live scores |
| **Summary** | Detailed game stats | Real-time during games | Limited (current season) | Box scores, player stats |
| **Teams** | Team information | Rarely (roster changes) | Static reference | Reference data, UI assets |
| **Standings** | Current standings | Daily | Current season only | League standings, playoff races |
| **Roster** | Player rosters | Weekly (transactions) | Current season | Player information, roster analysis |
| **News** | News articles | Multiple times daily | Several days | Content aggregation, news feeds |
| **Transactions** | Roster moves | As they occur | Full season history | Transaction tracking, roster changes |

---

## Rate Limiting Considerations by Endpoint

**High Frequency** (suitable for real-time polling):
- Scoreboard (during game days)
- Summary (during active games)

**Medium Frequency** (daily/weekly updates):
- Standings (daily)
- Roster (weekly)
- Transactions (daily)

**Low Frequency** (reference data):
- Teams (monthly or as needed)
- News (several times per day if needed)

---

## Notes and Limitations

1. **No official API documentation**: This is reverse-engineered from public endpoints
2. **No SLA or guarantees**: ESPN can change the API structure without notice
3. **Seasonal availability**: Some data only available during NBA season (October-June)
4. **Real-time delays**: Live game data may have 10-30+ second delays
5. **Historical data**: Limited historical data availability - focus on current season

## Related Endpoints (Not Fully Explored)

ESPN has additional NBA endpoints that may contain useful data but weren't fully explored:

- `/athletes/{PLAYER_ID}` - Individual player profiles and career statistics
- `/teams/{TEAM_ID}/schedule` - Team-specific schedules
- `/teams/{TEAM_ID}/statistics` - Team-specific season statistics  
- `/playoffs` - Playoff bracket and tournament information
- `/draft` - Draft information and prospect data
- `/statistics/leaders` - League statistical leaders
- `/injuries` - League-wide injury reports
- `/odds` - Betting odds and lines (availability varies)

**Exploration Tips**:
1. Start with `curl -s "URL" | jq 'keys'` to understand structure
2. Use team ID "1" (Atlanta Hawks) for testing team-specific endpoints
3. Player IDs can be found in roster or boxscore data
4. Some endpoints may require different API versions (`/apis/v2/` vs `/apis/site/v2/`)
5. Historical data availability varies significantly by endpoint

## API Discovery Pattern

For discovering new endpoints, try these common ESPN patterns:
```bash
# Base pattern
https://site.api.espn.com/apis/site/v2/sports/basketball/nba/{endpoint}

# Alternative version
https://site.api.espn.com/apis/v2/sports/basketball/nba/{endpoint}

# Team-specific pattern  
https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/{endpoint}

# Entity-specific pattern
https://site.api.espn.com/apis/site/v2/sports/basketball/nba/{endpoint}/{entity_id}
```

This documentation covers the most useful and reliable endpoints discovered through systematic API exploration. For production applications, focus on the core scoreboard and summary endpoints for game data, supplemented by teams and standings for reference information.