import pyspark as pys

from pyspark.sql.functions import udf

def load_s3_json(ftype, bucket, session):
    '''
    Extract file names stored in s3 bucket and return RDD
    '''
    data = session.read.json(bucket + '*.' + ftype)
    return(data)

def drop_missing(df):
    '''
    Remove completely blank rows from DataFrame
    '''
    return(df.where(df.HomeTeam.isNotNull()))

def convert_data_type(data, label):
    '''
    Convert strings to int
    '''
    return(data.withColumn(label, data[label].cast('int')))

def convert_columns(data):
    '''
    Iterate through columns and convert
    '''
    cnvrt = set(['Corners',
                 'Fouls',
                 'Goals',
                 'Cards',
                 'Shots',
                 'ShotsOnTarget',
                 'HalfTimeGoals',
                 'Team_Id'])

    for col in data.columns:
        col_reduced = col.replace('Away','').replace('Home','')

        if col_reduced in cnvrt:
            data = convert_data_type(data, col)

    return(data)

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

def team_metric_location(rdd, metric='YellowCards', location='Home'):
    '''
    Calculate metric average for teams given home or away
    '''
    lbl = location_label(location)

    val = rdd.map(lambda row: (row[lbl],(row[location + metric], 1))) \
             .reduceByKey(lambda (x, y): (x[0] + y[0], x[1] + y[1])) \
             .map(lambda (t, (m, g)): (t, f / float(g))) \
             .collectAsMap()

    return(val)

def team_metric_opponent(team_id, opp_id, rdd, metric='YellowCards', team_loc='Home'):
    '''
    Calculate metric average for team given opponent and location
    '''
    opp_loc = switch_label(team_loc)

    lbl_t = location_label(team_loc)
    lbl_o = location_label(opp_loc)

    val = rdd.filter(lambda row: (row[lbl_t] == team_id) & (row[lbl_o] == opp_id)) \
             .map(lambda row: (row[team_loc + metric], 1)) \
             .reduce(lambda c, p: (c[0] + p[0], c[1] + p[1]))

    return(val[0] / float(val[1]))

def extract_base_data(rdd):
    '''
    Extract data needed to create analytical base table
    (match_id, HomeTeam_Id, AwayTeam_Id, HomeYellowCards, AwayYellowCards)
    '''
    val = rdd.map(lambda row: (row['match_id'],
                              (row['HomeTeam_Id'],
                               row['AwayTeam_Id'],
                               row['HomeYellowCards'],
                               row['AwayYellowCards'])))

    return(val.collect())

def team_list(rdd):
    '''
    Collect team ids
    '''
    val = rdd.map(lambda row: row['HomeTeam_Id']) \
             .distinct() \
             .collect()

    return(val)

if __name__ == '__main__':
    #Initial Spark session
    sc = pys.SparkContext()
    session = pys.sql.SparkSession(sc)

    #Load data and prep
    data = load_s3_json('json', 's3://d-sparkbucket/fixtures/', session)
    data = drop_missing(data)
    data = convert_columns(data)

    #Switch DataFrame to RDD
    rdd = data.rdd

    #Collect team metric given location
    h_yellow = team_metric_location(rdd, metric='YellowCards', location='Home')

    print(h_yellow)
