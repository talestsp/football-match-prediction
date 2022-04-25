#RATING_COLS

home_team_history_rating_cols = ['home_team_history_rating_1', 'home_team_history_rating_2',
                                 'home_team_history_rating_3', 'home_team_history_rating_4',
                                 'home_team_history_rating_5', 'home_team_history_rating_6',
                                 'home_team_history_rating_7', 'home_team_history_rating_8',
                                 'home_team_history_rating_9', 'home_team_history_rating_10']

home_team_history_opponent_rating_cols = ['home_team_history_opponent_rating_1', 'home_team_history_opponent_rating_2',
                             'home_team_history_opponent_rating_3', 'home_team_history_opponent_rating_4',
                             'home_team_history_opponent_rating_5', 'home_team_history_opponent_rating_6',
                             'home_team_history_opponent_rating_7', 'home_team_history_opponent_rating_8',
                             'home_team_history_opponent_rating_9', 'home_team_history_opponent_rating_10']

away_team_history_rating_cols = ['away_team_history_rating_1', 'away_team_history_rating_2', 'away_team_history_rating_3',
                    'away_team_history_rating_4', 'away_team_history_rating_5', 'away_team_history_rating_6',
                    'away_team_history_rating_7', 'away_team_history_rating_8', 'away_team_history_rating_9',
                    'away_team_history_rating_10']

away_team_history_opponent_rating_cols = ['away_team_history_opponent_rating_1', 'away_team_history_opponent_rating_2',
                             'away_team_history_opponent_rating_3', 'away_team_history_opponent_rating_4',
                             'away_team_history_opponent_rating_5', 'away_team_history_opponent_rating_6',
                             'away_team_history_opponent_rating_7', 'away_team_history_opponent_rating_8',
                             'away_team_history_opponent_rating_9', 'away_team_history_opponent_rating_10']

team_history_rating_cols = home_team_history_rating_cols + home_team_history_opponent_rating_cols + away_team_history_rating_cols + away_team_history_opponent_rating_cols

######################################################################

# TEAM HISTORY MATCH DATES

home_history_match_date_cols = ['home_team_history_match_date_1', 'home_team_history_match_date_2',
                                'home_team_history_match_date_3', 'home_team_history_match_date_4',
                                'home_team_history_match_date_5', 'home_team_history_match_date_6',
                                'home_team_history_match_date_7', 'home_team_history_match_date_8',
                                'home_team_history_match_date_9', 'home_team_history_match_date_10']

away_history_match_date_cols = ['away_team_history_match_date_1', 'away_team_history_match_date_2',
                                'away_team_history_match_date_3', 'away_team_history_match_date_4',
                                'away_team_history_match_date_5', 'away_team_history_match_date_6',
                                'away_team_history_match_date_7', 'away_team_history_match_date_8',
                                'away_team_history_match_date_9', 'away_team_history_match_date_10']

team_history_match_date_cols = home_history_match_date_cols + away_history_match_date_cols

######################################################################

# TEAM HISTORY COACHES

home_team_history_coach_cols = ['home_team_history_coach_1', 'home_team_history_coach_2',
                                    'home_team_history_coach_3', 'home_team_history_coach_4',
                                    'home_team_history_coach_5', 'home_team_history_coach_6',
                                    'home_team_history_coach_7', 'home_team_history_coach_8',
                                    'home_team_history_coach_9', 'home_team_history_coach_10']

away_team_history_coach_cols = ['away_team_history_coach_1', 'away_team_history_coach_2',
                                    'away_team_history_coach_3', 'away_team_history_coach_4',
                                    'away_team_history_coach_5', 'away_team_history_coach_6',
                                    'away_team_history_coach_7', 'away_team_history_coach_8',
                                    'away_team_history_coach_9', 'away_team_history_coach_10']

team_history_coach_colnames = home_team_history_coach_cols + away_team_history_coach_cols

######################################################################

# TEAM HISTORY IS PLAY HOME

home_team_history_is_play_home_cols = ['home_team_history_is_play_home_1',
                                         'home_team_history_is_play_home_2',
                                         'home_team_history_is_play_home_3',
                                         'home_team_history_is_play_home_4',
                                         'home_team_history_is_play_home_5',
                                         'home_team_history_is_play_home_6',
                                         'home_team_history_is_play_home_7',
                                         'home_team_history_is_play_home_8',
                                         'home_team_history_is_play_home_9',
                                         'home_team_history_is_play_home_10']

away_team_history_is_play_home_cols = ['away_team_history_is_play_home_1',
                                         'away_team_history_is_play_home_2',
                                         'away_team_history_is_play_home_3',
                                         'away_team_history_is_play_home_4',
                                         'away_team_history_is_play_home_5',
                                         'away_team_history_is_play_home_6',
                                         'away_team_history_is_play_home_7',
                                         'away_team_history_is_play_home_8',
                                         'away_team_history_is_play_home_9',
                                         'away_team_history_is_play_home_10']

team_history_is_play_home_cols = home_team_history_is_play_home_cols + away_team_history_is_play_home_cols

######################################################################

# TEAM HISTORY IS CUP

home_team_history_is_cup_cols = ['home_team_history_is_cup_1',
                                         'home_team_history_is_cup_2',
                                         'home_team_history_is_cup_3',
                                         'home_team_history_is_cup_4',
                                         'home_team_history_is_cup_5',
                                         'home_team_history_is_cup_6',
                                         'home_team_history_is_cup_7',
                                         'home_team_history_is_cup_8',
                                         'home_team_history_is_cup_9',
                                         'home_team_history_is_cup_10']

away_team_history_is_cup_cols = ['away_team_history_is_cup_1',
                                         'away_team_history_is_cup_2',
                                         'away_team_history_is_cup_3',
                                         'away_team_history_is_cup_4',
                                         'away_team_history_is_cup_5',
                                         'away_team_history_is_cup_6',
                                         'away_team_history_is_cup_7',
                                         'away_team_history_is_cup_8',
                                         'away_team_history_is_cup_9',
                                         'away_team_history_is_cup_10']

team_history_is_cup_cols = home_team_history_is_cup_cols + away_team_history_is_cup_cols

######################################################################

# TEAM HISTORY LEAGUE ID

home_team_history_league_id_cols = ['home_team_history_league_id_1',
                                     'home_team_history_league_id_2',
                                     'home_team_history_league_id_3',
                                     'home_team_history_league_id_4',
                                     'home_team_history_league_id_5',
                                     'home_team_history_league_id_6',
                                     'home_team_history_league_id_7',
                                     'home_team_history_league_id_8',
                                     'home_team_history_league_id_9',
                                     'home_team_history_league_id_10']

away_team_history_league_id_cols = ['away_team_history_league_id_1',
                                     'away_team_history_league_id_2',
                                     'away_team_history_league_id_3',
                                     'away_team_history_league_id_4',
                                     'away_team_history_league_id_5',
                                     'away_team_history_league_id_6',
                                     'away_team_history_league_id_7',
                                     'away_team_history_league_id_8',
                                     'away_team_history_league_id_9',
                                     'away_team_history_league_id_10']

team_history_league_id_cols = home_team_history_league_id_cols + away_team_history_league_id_cols

######################################################################

# TEAM HISTORY GOAL

home_team_history_goal_cols = ['home_team_history_goal_1',
                                 'home_team_history_goal_2',
                                 'home_team_history_goal_3',
                                 'home_team_history_goal_4',
                                 'home_team_history_goal_5',
                                 'home_team_history_goal_6',
                                 'home_team_history_goal_7',
                                 'home_team_history_goal_8',
                                 'home_team_history_goal_9',
                                 'home_team_history_goal_10']

home_team_history_opponent_goal_cols = [ 'home_team_history_opponent_goal_1',
                                         'home_team_history_opponent_goal_2',
                                         'home_team_history_opponent_goal_3',
                                         'home_team_history_opponent_goal_4',
                                         'home_team_history_opponent_goal_5',
                                         'home_team_history_opponent_goal_6',
                                         'home_team_history_opponent_goal_7',
                                         'home_team_history_opponent_goal_8',
                                         'home_team_history_opponent_goal_9',
                                         'home_team_history_opponent_goal_10']

away_team_history_goal_cols = ['away_team_history_goal_1',
                                'away_team_history_goal_2',
                                'away_team_history_goal_3',
                                'away_team_history_goal_4',
                                'away_team_history_goal_5',
                                'away_team_history_goal_6',
                                'away_team_history_goal_7',
                                'away_team_history_goal_8',
                                'away_team_history_goal_9',
                                'away_team_history_goal_10']

away_team_history_opponent_goal_cols = ['away_team_history_opponent_goal_1',
                                            'away_team_history_opponent_goal_2',
                                            'away_team_history_opponent_goal_3',
                                            'away_team_history_opponent_goal_4',
                                            'away_team_history_opponent_goal_5',
                                            'away_team_history_opponent_goal_6',
                                            'away_team_history_opponent_goal_7',
                                            'away_team_history_opponent_goal_8',
                                            'away_team_history_opponent_goal_9',
                                            'away_team_history_opponent_goal_10']

team_history_goal_cols = home_team_history_goal_cols + home_team_history_opponent_goal_cols + away_team_history_goal_cols + away_team_history_opponent_goal_cols