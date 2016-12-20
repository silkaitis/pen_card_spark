import pyspark as pys

def load_s3_json(ftype, bucket, session):
    '''
    Extract file names stored in s3 bucket
    '''
    data = session.read.json(bucket + '*.' + ftype)
    return(data)

def drop_missing(df):
    '''
    Remove completely blank rows from DataFrame
    '''
    return(df.where(df.HomeTeam.isNotNull()))

def team_metric_location(team_id, metric='YellowCards', location='Home', rdd):
    '''
    Calculate metric average for team given home or away
    '''
    if location == 'Home':
        lbl = 'HomeTeam_Id'
    else:
        lbl = 'AwayTeam_Id'

    val = rdd.filter(lambda row: row[lbl] == team_id) \
             .map(lambda row: (int(row[location + metric]), 1)) \
             .reduce(lambda c, p: (c[0] + p[0], c[1] + p[1]))

    return(val[0] / float(val[1]))

def team_metric_opponent(team_id, opp_id, rdd):
    '''
    Calculate average number of yellow cards given an opponent
    '''
    pass

if __name__ == '__main__':
    #Do stuff
