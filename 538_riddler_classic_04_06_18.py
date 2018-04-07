from pandas.io import gbq
from graphviz import Digraph

project_id = '[YOUR_PROJECT_ID]'
project_id = 'ajarvis-test'

def get_matchups(markets, its):
  teams = ""
  for x in markets:
    teams += ', "' + str(x) + '"'
  teams = "(" + teams[2:] + ")"
  
  query = """
  SELECT
    games.market,
    colors.color,
    games.opp_market,
    %s AS iterations
  FROM
    `bigquery-public-data.ncaa_basketball.mbb_teams_games_sr` AS games
  LEFT JOIN
    `bigquery-public-data.ncaa_basketball.team_colors` AS colors
  ON
    games.market = colors.market
  WHERE
    season = 2017
    AND division_alias = "D1"
    AND opp_division_alias = "D1"
    AND points_game < opp_points_game
    AND games.market IN %s
    AND games.opp_market NOT IN %s
  GROUP BY
    games.market,
    colors.color,
    games.opp_market
  ORDER BY
    games.opp_market
    """%(str(its), teams, teams)

  results = gbq.read_gbq(query=query, dialect ='standard', project_id=project_id)
  return results

def get_colors():
  query = """
  SELECT
    market,
    color
  FROM
    `bigquery-public-data.ncaa_basketball.team_colors` AS colors
    """

  results = gbq.read_gbq(query=query, dialect ='standard', project_id=project_id)
  return results

matchups = get_matchups(["Villanova"],1)
results = matchups.copy(deep=True)

# new = ["Villanova"]
# for x in matchups.opp_market:
#   new.append(x)

iterations = 2
while len(results) > 0:
  new = ["Villanova"]
  for x in matchups.opp_market:
    new.append(x)
  print new
  results = get_matchups(new, iterations)
  matchups = matchups.append(results, ignore_index=True)

  iterations += 1
  
u = Digraph('Riddler_04_06_18', filename='graph.gv', format='png');
u.graph_attr['rankdir'] = 'LR'
u.graph_attr['ranksep'] = "6"

nodes = get_colors()
for market, t_color in zip(nodes.market, nodes.color):
  u.node(market, color=t_color)

for market,opp_market,l_color in zip(matchups.market,matchups.opp_market,matchups.color):
  u.node(market, )
  u.edge(market, opp_market, color=l_color)
u.render()

