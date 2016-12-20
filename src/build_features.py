import pyspark as pys

def load_s3_json(ftype, bucket, session):
    '''
    Extract file names stored in s3 bucket and return RDD
    '''
    data = session.read.json(bucket + '*.' + ftype)
    return(data.rdd)

def drop_missing(df):
    '''
    Remove completely blank rows from DataFrame
    '''
    return(df.where(df.HomeTeam.isNotNull()))

def location_label(location):
    '''
    Create dictionary label
    '''
    if location == 'Home':
        return('HomeTeam_Id')
    else:
        return('AwayTeam_Id')

def switch_label(location):
    '''
    Switch label
    '''
    d = {'Home': 'Away', 'Away': 'Home'}

    return(d[location])

def team_metric_location(team_id, metric='YellowCards', location='Home', rdd):
    '''
    Calculate metric average for team given home or away
    '''
    lbl = location_label(location)

    val = rdd.filter(lambda row: row[lbl] == team_id) \
             .map(lambda row: (int(row[location + metric]), 1)) \
             .reduce(lambda c, p: (c[0] + p[0], c[1] + p[1]))

    return(val[0] / float(val[1]))

def team_metric_opponent(team_id, opp_id, metric='YellowCards', team_loc='Home', rdd):
    '''
    Calculate metric average for team given opponent and location
    '''
    opp_loc = switch_label(team_loc)

    lbl_t = location_label(team_loc)
    lbl_o = location_label(opp_loc)

    val = rdd.filter(lambda row: (row[lbl_t] == team_id) & (row[lbl_o] == opp_id)) \
             .map(lambda row: (int(row[team_loc + metric]), 1)) \
             .reduce(lambda c, p: (c[0] + p[0], c[1] + p[1]))

    return(val[0] / float(val[1]))

def extract_base_data(rdd):
    '''
    Extract data needed to create analytical base table
    (match_id, HomeTeam_Id, AwayTeam_Id, HomeYellowCards, AwayYellowCards)
    '''
    val = rdd.map(lambda row: (int(row['match_id']),
                              (int(row['HomeTeam_Id']),
                               int(row['AwayTeam_Id']),
                               int(row['HomeYellowCards']),
                               int(row['AwayYellowCards']))))

    return(val)

if __name__ == '__main__':
    #Do stuff
