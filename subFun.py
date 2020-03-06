"""
__author__ = ["Junfeng Zhu", "Nathan Tregger","Rob Buse", "Mark Roberts"]
__credits__ = ["Junfeng Zhu", "Nathan Tregger","Rob Buse", "Greg Goldstein
", "Prakash Poudel","Lee Eastburn", "Mark Roberts", "Joseph Reynolds "]
__version__ = "2.2"
__maintainer__ = ["Junfeng Zhu"]
__email__ = "Junfeng.zhu@gcpat.com"
aa
"""

def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

import numpy as np
import pandas as pd
import statsmodels.api as sm
#from sshtunnel import SSHTunnelForwarder
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
from datetime import datetime

import time

from scipy.stats import norm, chi2
import random

from sklearn.metrics.pairwise import pairwise_distances
from sklearn.manifold import TSNE
from sklearn.cluster import SpectralClustering
from sklearn.cluster import KMeans
from sklearn.preprocessing import normalize
from sklearn.preprocessing import minmax_scale
from matplotlib.patches import Ellipse

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
import seaborn as sns
from sklearn.linear_model import LinearRegression

from numba import jit
import math

import platform

import win32com.client as win32
def read_SCDwithTicket_B(cursor, start="", end=str(pd.datetime.now().date() + pd.Timedelta(days=1))):
#    WHERE ti.load_size > 4.5 AND ti.load_size < 7.8 AND scd.load_size_m3 > 4.5 \
#    WHERE ti.load_size > 0 AND scd.load_size_m3 > 0 \
    frames = pd.DataFrame()
    sqlStr = '''SELECT scd.first_psi,           scd.first_rpm,              scd.second_psi,       scd.second_rpm,\
                       scd.truck_id,            scd.ticket_id,              scd.message_time_utc, scd.load_size_m3,\
                       scd.truck_tilt_degrees,  scd.estimated_buildup_lbs,  ti.mix_code_id,       ti.plant_id,\
                       ti.load_size,            ti.location_id,             ti.max_water_override\
                FROM web.speed_change_data scd INNER JOIN web.ticket ti \
                ON scd.ticket_id = ti.id AND scd.truck_id = ti.truck_id \
                WHERE ti.load_size > 0 AND scd.load_size_m3 > 0 \
                AND scd.first_psi > 0 AND scd.second_psi > 0 \
                AND scd.first_rpm > 0 AND scd.second_rpm > 0 \
                AND scd.message_time_utc >= '%s' AND scd.message_time_utc < '%s';'''

    colNames=[       'first_psi',             'first_rpm',                'second_psi',         'second_rpm',
                     'TrID',                  'TicketID',                 'TimeUTC',            'LSscd',
                     'Tilt',                  'Buildup',                  'MixID',              'PlantID', 
                     'LoadSize',              'LocID',                    'MWO']

    if not start:
        end_date   = datetime.strptime(end,   '%Y-%m-%d').date()
        while True:
            start_date = end_date - pd.Timedelta(weeks=6)
#            start_date = end_date - pd.Timedelta(days=1)
            cursor.execute(sqlStr % (str(start_date), str(end_date)))
            x = cursor.fetchall()
            df = pd.DataFrame(data=x, columns=colNames)
            if df.shape[0] == 0: break
            end_date = start_date
            frames = frames.append(df)
    else:
        start_date = datetime.strptime(start, '%Y-%m-%d').date()
        end_date   = datetime.strptime(end,   '%Y-%m-%d').date()
        while True:
            start_dateT = end_date - pd.Timedelta(weeks=6)
#            start_dateT = end_date - pd.Timedelta(days=1)
            if (start_date < start_dateT):
                cursor.execute(sqlStr % (str(start_dateT), str(end_date)))
                x = cursor.fetchall()
                df = pd.DataFrame(data=x, columns=colNames)
                end_date = start_dateT
                frames = frames.append(df)
            else:
                cursor.execute(sqlStr % (str(start_date), str(end_date)))
                x = cursor.fetchall()
                df = pd.DataFrame(data=x, columns=colNames)
                end_date = start_date
                frames = frames.append(df)
                break
    frames=frames.reset_index(drop=True)
    return frames

def read_SCDwithTicket(cursor, N=52, start="", end=""):
    frames = pd.DataFrame()
    if not start:
        start = str(pd.datetime.now().date() - pd.Timedelta(weeks=52*10))
    if not end:
        end = str(pd.datetime.now().date() + pd.Timedelta(days=1))
        
    sqlStr = '''SELECT scd.first_psi,           scd.first_rpm,              scd.second_psi,       scd.second_rpm,\
  scd.truck_id,            scd.ticket_id,              scd.message_time_utc, scd.load_size_m3,\
  scd.truck_tilt_degrees,  scd.estimated_buildup_lbs,  ti.mix_code_id,       ti.plant_id,\
  ti.load_size,            ti.location_id,             ti.max_water_override\
  FROM web.speed_change_data scd INNER JOIN web.ticket ti \
  ON scd.ticket_id = ti.id AND scd.truck_id = ti.truck_id \
  WHERE ti.load_size > 0 AND scd.load_size_m3 > 0 \
  AND scd.first_psi > 0 AND scd.second_psi > 0 \
  AND scd.first_rpm > 0 AND scd.second_rpm > 0  \
  AND scd.message_time_utc >= '%s' AND scd.message_time_utc < '%s';'''
    colNames=[       'first_psi',             'first_rpm',                'second_psi',         'second_rpm',
                   'TrID',                  'TicketID',                 'TimeUTC',            'LSscd',
                   'Tilt',                  'Buildup',                  'MixID',              'PlantID', 
                   'LoadSize',              'LocID',                    'MWO']
    
    end_date   = datetime.strptime(end,   '%Y-%m-%d').date()
    startT = str(end_date - pd.Timedelta(weeks=N))
    if start > startT:
        startT = start
    while start <= startT:
        print("............reading: "+startT+" to "+end)
        cursor.execute(sqlStr % (startT, end))
        x = cursor.fetchall()
        frameT = pd.DataFrame(data=x, columns=colNames)
        if frameT.shape[0] == 0:
            break
        frames = frames.append(frameT)
        end = startT
        end_date   = datetime.strptime(end,   '%Y-%m-%d').date()
        startT = str(end_date - pd.Timedelta(weeks=N))
        
    return frames

def read_PlantInfo(cursor):
    sqlStr ='''SELECT  id, name,  location_id \
               FROM web.plant;'''

    colNames=['PlantID','PlantName','LocID']
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    framePlant = pd.DataFrame(data=x, columns=colNames)
    framePlant = framePlant.sort_values(['PlantID'])
    return framePlant

def read_TruckInfo(cursor):
#    tr.is_deleted-----------# do we need a filter on tr.is_deleted?
    sqlStr ='''SELECT  tr.id,             tr.name,        tr.truck_type_id,\
                       tt.name,           tt.account_id,\
                       acc.account_name,  acc.is_archived \
               FROM web.truck tr INNER JOIN web.truck_type tt ON tr.truck_type_id = tt.id\
                                 INNER JOIN web.account acc on tt.account_id = acc.id;'''

    colNames=[        'TrID',            'TrName',       'TrTypeID',
                      'TrTypeName',      'AccID',
                      'AccName',         'Archived_Acc']
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameTr = pd.DataFrame(data=x, columns=colNames)
    
    return frameTr

def read_AccountInfo(cursor):
    sqlStr ='''SELECT  acc.id, acc.account_name,  acc.is_archived \
               FROM web.account acc ;'''

    colNames=['AccID','AccName','Archived_Acc']
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameAcc = pd.DataFrame(data=x, columns=colNames)
    frameAcc = frameAcc.sort_values(['AccID'])
    return frameAcc

def read_MixCodeInfo(cursor):
    
    sqlStr = '''SELECT id,      name,  account_id, slump_curve_id, is_archived \
                FROM web.mix_code;'''

    colNames=[        'MixID', 'Mix', 'AccID',    'ScID',         'Archived']
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameMix = pd.DataFrame(data=x, columns=colNames)
    
    return frameMix

def read_LocationInfo(cursor):
    
    sqlStr = '''SELECT id,      name,      account_id\
                FROM web.location;'''

    colNames=[        'LocID', 'LocName', 'AccID']
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameLoc = pd.DataFrame(data=x, columns=colNames)
    
    return frameLoc

def read_SCCInfo(cursor):
    
    sqlStr = '''SELECT id,         name,      slump_curve_id, truck_type_id, do_not_measure, \
                       constant,   ps_log,    ps_linear,      spd_square,    spd_linear,   spd_dividing\
                FROM web.slump_curve_coefficients;'''

    colNames=[        "SccID",    "SccName", "ScID",         "TrTypeID",    "DNM", 
                      "constant", "ps_log",  "ps_linear",    "spd_square",  "spd_linear", "spd_dividing"]
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameSCC = pd.DataFrame(data=x, columns=colNames)
    
    return frameSCC


def read_SCInfo(cursor):
    
    sqlStr = '''SELECT id,         account_id, name\
                FROM web.slump_curve;'''

    colNames=[        "ScID",     "AccID",     "ScName"]
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameSC = pd.DataFrame(data=x, columns=colNames)
    
    return frameSC

def read_ACSCInfo(cursor):
    
    sqlStr = '''SELECT account_id, mix_code_id, truck_type_id, slump_curve_coefficients_id\
                FROM web.auto_calibration_slump_coefficients;'''

    colNames=[        "AccID",    "MixID",     "TrTypeID",    "SccID"]
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameACSC = pd.DataFrame(data=x, columns=colNames)
    
    return frameACSC

def read_InstructionAssignmentInfo(cursor):

    sqlStr = '''SELECT account_id, mix_code_id, location_id, do_not_measure, instruction_assignment_type_id\
                FROM web.instruction_assignment;'''

    colNames=[        "AccID",    "MixID",     "LocID",     "DNM",          "insAssnTypeID"]
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameInstrAss = pd.DataFrame(data=x, columns=colNames)
    
    return frameInstrAss

def mergeWithInstructionAssignment(frames, web_instruction_assignment):
    web_instruction_assignment = web_instruction_assignment.rename(columns={"DNM":"dnmIA"})
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==3 ][['MixID', 'LocID', 'dnmIA']].drop_duplicates(), on = ['LocID','MixID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    # frames.DNM.isna().sum() # 1077199 # 1035290
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==7 ][['MixID', 'LocID', 'dnmIA']].drop_duplicates(), on = ['LocID','MixID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    # frames.DNM.isna().sum() # 1035290 # 1035290
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==2 ][['MixID', 'dnmIA']].drop_duplicates(), on = ['MixID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    # frames.DNM.isna().sum() # 1035290 # 183270
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==4 ][['MixID', 'dnmIA']].drop_duplicates(), on = ['MixID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    # frames.DNM.isna().sum() # 183270 # 102540
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==1 ][['LocID', 'dnmIA']].drop_duplicates(), on = ['LocID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    # frames.DNM.isna().sum() # 102540 # 36532
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==6 ][['LocID', 'dnmIA']].drop_duplicates(), on = ['LocID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    #frames.DNM.isna().sum() # 36532 # 35457
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==0 ][['AccID', 'dnmIA']].drop_duplicates(), on = ['AccID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    #frames.DNM.isna().sum() # 35457 # 6173
    
    frames = pd.merge(frames, web_instruction_assignment[web_instruction_assignment.insAssnTypeID ==5 ][['AccID', 'dnmIA']].drop_duplicates(), on = ['AccID'], how = 'left')
    frames['DNM'] = np.where(frames.DNM.isnull(), frames.dnmIA, frames.DNM)
    frames = frames.loc[:,~(frames.columns.isin(['dnmIA']))]
    #frames.DNM.isna().sum() # 6173 # 0
    
    return frames


def read_MixInfo():
    MixInfo = pd.read_csv('MixInfo_V1.csv')
    MixInfo = pd.DataFrame(MixInfo)

    return MixInfo

def GenerateEllipse(df, plot=False,i=1):
    pos = df.mean(axis=0) # center coordinates
#    cov = np.cov(df, rowvar=False) # covariance ,columnwise
    cov = np.cov(df.astype(float), rowvar=False) # for Rob
    vals, vecs = np.linalg.eigh(cov) # eigenvalues and eigenvectors
    order = vals.argsort()[::-1]  # order the eigenvalues
    vals, vecs =vals[order], vecs[:,order]
    theta = np.degrees(np.arctan2(*vecs[:,0][::-1])) # rotation degree
    #q = 2 * norm.cdf(3) - 1
    q=0.9                  # quantile
    r2 = chi2.ppf(q, 2)
    width, height = 2 * np.sqrt(vals * r2) # short and long axis
    
# plot the ellipse
    if plot == True:
        fig = plt.figure(i)
        plt.plot(df.iloc[:,0], df.iloc[:,1], 'go',alpha = 0.1)
        ellip = Ellipse(xy=pos, width=width, height=height, angle=theta) 
        ax = plt.gca()
        ax.add_artist(ellip)
        ellip.set_clip_box(ax.bbox)
        ellip.set_alpha(np.random.rand())

#    return (pos[0],pos[1], width, height, theta, cov)
    return (pos[0],pos[1], width, height, theta)

## k-medoid clustering
def assign_points_to_clusters(medoids, distances):
    distances_to_medoids = distances[:,medoids]
    clusters = medoids[np.argmin(distances_to_medoids, axis=1)]
    clusters[medoids] = medoids
    return clusters

def compute_new_medoid(cluster, distances):
    mask = np.ones(distances.shape)
    mask[np.ix_(cluster,cluster)] = 0.
    cluster_distances = np.ma.masked_array(data=distances, mask=mask, fill_value=10e9)
    costs = cluster_distances.sum(axis=1)
    return costs.argmin(axis=0, fill_value=10e9)

def kMedoids1(distances, k=3):

    m = distances.shape[0] # number of points

    # Pick k random medoids.
    curr_medoids = np.array([-1]*k)
    while not len(np.unique(curr_medoids)) == k:
        curr_medoids = np.array([random.randint(0, m - 1) for _ in range(k)])
    old_medoids = np.array([-1]*k) # Doesn't matter what we initialize these to.
    new_medoids = np.array([-1]*k)
   
    # Until the medoids stop updating, do the following:
    while not ((old_medoids == curr_medoids).all()):
        # Assign each point to cluster with closest medoid.
        clusters = assign_points_to_clusters(curr_medoids, distances)

        # Update cluster medoids to be lowest cost point. 
        for curr_medoid in curr_medoids:
            cluster = np.where(clusters == curr_medoid)[0]
            new_medoids[curr_medoids == curr_medoid] = compute_new_medoid(cluster, distances)

        old_medoids[:] = curr_medoids[:]
        curr_medoids[:] = new_medoids[:]

    return clusters, curr_medoids

def kMedoids(D, k, tmax=100):
    # determine dimensions of distance matrix D
    m, n = D.shape

    if k > n:
        raise Exception('too many medoids')

    # find a set of valid initial cluster medoid indices since we
    # can't seed different clusters with two points at the same location
    valid_medoid_inds = set(range(n))
    invalid_medoid_inds = set([])
    rs,cs = np.where(D==0)
    # the rows, cols must be shuffled because we will keep the first duplicate below
    py_version = platform.python_version()
    py_version = py_version.split('.')[0]
    if py_version == '3':
        index_shuf = list(range(len(rs)))
    elif py_version == '2':
        index_shuf = range(len(rs))
    else:
        print('Python version???')
#    index_shuf = list(range(len(rs)))  # py3: list(range(len(rs)))   ## py2: range(len(rs))
    np.random.shuffle(index_shuf)
    rs = rs[index_shuf]
    cs = cs[index_shuf]
    for r,c in zip(rs,cs):
        # if there are two points with a distance of 0...
        # keep the first one for cluster init
        if r < c and r not in invalid_medoid_inds:
            invalid_medoid_inds.add(c)
    valid_medoid_inds = list(valid_medoid_inds - invalid_medoid_inds)

    if k > len(valid_medoid_inds):
        raise Exception('too many medoids (after removing {} duplicate points)'.format(
            len(invalid_medoid_inds)))

    # randomly initialize an array of k medoid indices
    M = np.array(valid_medoid_inds)
    np.random.shuffle(M)
    M = np.sort(M[:k])

    # create a copy of the array of medoid indices
    Mnew = np.copy(M)

    # initialize a dictionary to represent clusters
    C = {}
    for t in range(tmax):   # for t in xrange(tmax)
        # determine clusters, i. e. arrays of data indices
        J = np.argmin(D[:,M], axis=1)
        for kappa in range(k):
            C[kappa] = np.where(J==kappa)[0]
        # update cluster medoids
        for kappa in range(k):
            J = np.mean(D[np.ix_(C[kappa],C[kappa])],axis=1)
            j = np.argmin(J)
            Mnew[kappa] = C[kappa][j]
        np.sort(Mnew)
        # check for convergence
        if np.array_equal(M, Mnew):
            break
        M = np.copy(Mnew)
    else:
        # final update of cluster memberships
        J = np.argmin(D[:,M], axis=1)
        for kappa in range(k):
            C[kappa] = np.where(J==kappa)[0]

    # return results
    return M, C

def ClusteringAlgorithms(framesT, algo = "kMedoids", features = ['meanX','meanY','height'], k = 75):
    ## add ellipse info
    framesT1 = framesT.groupby(['MixID','TrTypeID']).agg({'TrTypeID':'size'}) \
               .rename(columns={'TrTypeID':'n1'}).reset_index() 
    framesT = framesT1.merge(framesT,on = ['MixID','TrTypeID'],how="left")
    
#    a= framesT.shape[0]
#    b=framesT[framesT.n1 >= 10].shape[0]
#    framesT['KeepInd'] = np.where(( (framesT.EnableAccAC == 1)&(framesT.n1>=20) )|( (framesT.EnableAccAC == 2)&(framesT.n1>=10) ), 1, 0)
#    framesT = framesT[framesT.KeepInd == 1]
#    c= framesT.shape[0]
#    print("start: "+str(a)+"; before: "+str(b)+"; now: "+str(c))
#    framesT = framesT[['MixID', 'TrTypeID','SlopeL', 'InterceptL']]  
##    framesT = framesT[framesT.n1 >= 20][['MixID', 'TrTypeID','SlopeL', 'InterceptL']]       ##  10
##    framesT = framesT[framesT.n1 >= 20][['MixID', 'TrTypeID','SlopeL', 'InterceptL']]       ##  10
    
    framesT = framesT[framesT.n1 >= 8][['MixID', 'TrTypeID','SlopeL', 'InterceptL']]    
    
    framesT['SlopeL'] = framesT.SlopeL*30
    MixTrEllipse = framesT.groupby(['MixID','TrTypeID']).apply(GenerateEllipse).reset_index()
    MixTrEllipse.columns=['MixID','TrTypeID','All']
    MixTrEllipse[['meanX','meanY','width','height','theta']] = MixTrEllipse['All'].apply(pd.Series)
    MixTrEllipse = MixTrEllipse[['MixID','TrTypeID','meanX','meanY','width','height','theta']]
    
    ## kill (Mix, Tr) with theta <= 0
    ## some (Mix, Tr) with more than 10 may not get a slump curve------BE CAREFUL
    MixTrEllipse = MixTrEllipse[MixTrEllipse.theta > 0].reset_index()
    
    MixTrEllipse['tanTheta'] = np.tan(MixTrEllipse.theta*np.pi/180)
    
    MixTrPairs = MixTrEllipse[['MixID', 'TrTypeID']]
    
    X = pd.DataFrame(minmax_scale(MixTrEllipse[features]))
    X.columns=features
    
    if algo == "kMedoids":
        kmedoidObj = kMedoids(pairwise_distances(X,metric='euclidean'), k=k, tmax=100)
        labels=pd.DataFrame(kmedoidObj[1].items())
        labels.columns = ['Group', 'Data']
        labels=labels.Data.apply(pd.Series).stack().reset_index().iloc[:,[0,2]]
        labels.columns = ['Labels','index']
        labels=labels.set_index('index')
        labels=labels.sort_index()
        
        labels = pd.concat([MixTrPairs, labels],sort = False, axis = 1)
        
        return labels
    
    elif algo == "KMeans":
        labels = pd.DataFrame(KMeans(n_clusters = k, random_state=0).fit_predict(X))
        
        labels = pd.concat([MixTrPairs, labels],sort = False, axis = 1)
        
        return labels
    
    elif algo == "SpectralClustering":
        model = SpectralClustering(n_clusters=k, affinity='nearest_neighbors',assign_labels='kmeans')
        labels = pd.DataFrame(model.fit_predict(X))
        labels1 = pd.concat([MixTrEllipse, labels],sort = False, axis = 1)
        labels = pd.concat([MixTrPairs, labels],sort = False, axis = 1)
        labels.columns = ['MixID','TrTypeID','Labels']
        return labels
    
    elif algo == "tsne":
        ## tsne
        tsne = TSNE(n_components=3, init='random', random_state=0)
        MixTrEllipse_proj = tsne.fit_transform(X)
        # Compute the clusters
        kmeans = KMeans(n_clusters=k, random_state=0)
        labels = pd.DataFrame(kmeans.fit_predict(MixTrEllipse_proj))
        
        labels = pd.concat([MixTrPairs, labels],sort = False, axis = 1)
        
        return labels
    
    elif algo == "GregKmeans":
        return labels
    
    else:                                   
        print("Check your clustering algorithm")
        return -1

def appFunction(df):
    df1 = df[df.loRpm <= 3]
    if df1.shape[0] >= 100:
        model = LinearRegression(fit_intercept=True).fit(df1.loc[:, ['InterceptL']], df1.loc[:, 'lowSlump'])
        df['pred']=model.predict(df.loc[:, ['InterceptL']])
    else:
        df['pred'] = df.lowSlump
    return df

def slumpModels(df):
# =============================================================================
#     df1 = df[['hiRpm', 'HiLogP','lowSlumpTr']].rename(columns={"hiRpm":"Rpm", "HiLogP":"LogP"}).reset_index(drop=True)
# 
#     df2 = df[['loRpm', 'LoLogP','lowSlumpTr']].rename(columns={"loRpm":"Rpm", "LoLogP":"LogP"}).reset_index(drop=True)
# 
#     df3 = df1.append(df2, sort=False)
#     df3 = df3.assign(SR = df3.lowSlumpTr*df3.Rpm)
# =============================================================================
    
# =============================================================================
#     df1 = df[['hiRpm', 'HiLogP','lowSlump']].rename(columns={"hiRpm":"Rpm", "HiLogP":"LogP"}).reset_index(drop=True)
# 
#     df2 = df[['loRpm', 'LoLogP','lowSlump']].rename(columns={"loRpm":"Rpm", "LoLogP":"LogP"}).reset_index(drop=True)
# 
#     df3 = df1.append(df2, sort=False)
#     df3 = df3.assign(SR = df3.lowSlump*df3.Rpm)
# =============================================================================
    
#    df['lowSlump'] = np.where(df.lowSlump < 0, 0, np.where(df.lowSlump >= 11, 11, df.lowSlump))
    df1 = df[['hiRpm', 'HiLogP','lowSlump']].rename(columns={"hiRpm":"Rpm", "HiLogP":"LogP"}).reset_index(drop=True)

    df2 = df[['loRpm', 'LoLogP','lowSlump']].rename(columns={"loRpm":"Rpm", "LoLogP":"LogP"}).reset_index(drop=True)

    df3 = df1.append(df2, sort=False)
    df3 = df3.assign(SR = df3.lowSlump*df3.Rpm)
    
    model = LinearRegression(fit_intercept=True).fit(df3.iloc[:, [1,3]], df3.iloc[:, 2])
    
    return model.intercept_, model.coef_[0], -model.coef_[1]

def slumpCoefficients(framesT):
    
    ModelCoef = framesT.groupby(['Labels']).apply(slumpModels).reset_index()
    
    ModelCoef.columns=['Labels','All']
    
    ModelCoef[['coef1','coef2','coef3']] = ModelCoef['All'].apply(pd.Series)
    ModelCoef = ModelCoef.drop(columns=['All'])
    
    ModelCoef['constant_AC']     = ModelCoef.coef1
    ModelCoef['ps_log_AC']       = ModelCoef.coef2
    ModelCoef['ps_linear_AC']    = 0
    ModelCoef['spd_square_AC']   = 0
    ModelCoef['spd_linear_AC']   = 0
    ModelCoef['spd_dividing_AC'] = ModelCoef.coef3
    
    ModelCoef = ModelCoef.drop(columns=['coef1','coef2','coef3'])
    
    return ModelCoef

def boxPlotFilter(df, col = 'mean_All'):
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1
    minV = Q1 - (IQR*1.5)
    maxV = Q3 + (IQR*1.5)
    df = df[(df[col] >= minV)&(df[col]<=maxV)]
    return df

def similarTrucks(df):
    df1 = df.groupby(['TrTypeIDInd'],as_index=False).apply(boxPlotFilter).groupby(['TrTypeIDInd']).aggregate({'mean_All':[np.mean, np.std]})
    df1.columns = ['mean', 'std']
#    df1=df1.sort_values(['std'], ascending=[True]).reset_index(drop=False).iloc[[0],[0,1]]
    
    return df1


def ModelStage2(df, Model3):
    df = df.values.tolist()
    
    # [u'MixID', u'TrTypeID', u'Labels', u'n', u'TrTypeName', u'AccName', u'ModelInd', u'V1', u'V2', u'CIS', u'WC']
    #  0            1           2         3       4            5           6            7      8      9       10
    if pd.isnull(df[9]):    # no CIS info
        df1 = Model3[(Model3.AccName == df[5])&(Model3.TrTypeName == df[4] )]
        if df1.shape[0] == 0:    # no CIS info and no common (TrTypeName, AccName)
            df1 = Model3[(Model3.TrTypeName == df[4] )]
            if df1.shape[0] == 0:    # no CIS info and no TrTypeName
                Labels = -1
            else:                   # no CIS info, but with TrTypeName (different AccName)
                df2 = df1.groupby(['Labels']).size().reset_index()
                df2.columns = ['Labels', 'num']
                df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
                Labels = df2.loc[0,'Labels']
        else:                       # no CIS info, but with  (TrTypeName, AccName)
            df2 = df1.groupby(['Labels']).size().reset_index()
            df2.columns = ['Labels', 'num']
            df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
            Labels = df2.loc[0,'Labels']
    else: # with CIS info
        df1 = Model3[Model3.CIS == df[9]]
        if df1.shape[0] == 0:        # "no" CIS info 
            df1 = Model3[(Model3.AccName == df[5])&(Model3.TrTypeName == df[4] )]
            if df1.shape[0] == 0:    # "no" CIS info and no common (TrTypeName, AccName)
                df1 = Model3[(Model3.TrTypeName == df[4] )]
                if df1.shape[0] == 0:    # "no" CIS info and no TrTypeName
                    Labels = -1
                else:                   # "no" CIS info, but with TrTypeName (different AccName)
                    df2 = df1.groupby(['Labels']).size().reset_index()
                    df2.columns = ['Labels', 'num']
                    df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
                    Labels = df2.loc[0,'Labels']
            else:                       # "no" CIS info, but with  (TrTypeName, AccName)
                df2 = df1.groupby(['Labels']).size().reset_index()
                df2.columns = ['Labels', 'num']
                df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
                Labels = df2.loc[0,'Labels']
            
        else: ## find the one with the highest wc
            df1[['V1']] = df1[['V1']] - df[7]
            df1[['V2']] = df1[['V2']] - df[8]
            df1[['WC']] = df1[['WC']] - df[10]
#            df1['MixInfoInd'] = np.where(( (df1.V1 <= 0)&(df1.V2 >= 0) )|(np.abs(df1.WC)<0.1),1,0)
#            df1.loc[:,'MixInfoInd'] = np.where(( (df1.V1 <= 0)&(df1.V2 >= 0) )|(np.abs(df1.WC)<0.1),1,0)
            df1 =df1.assign(MixInfoInd = np.where(( (df1.V1 <= 0)&(df1.V2 >= 0) )|(np.abs(df1.WC)<0.1),1,0))
            df2 = df1[df1.MixInfoInd == 1]
            if df2.shape[0]==0:  # same CIS but different WC
                
                df1 = Model3[(Model3.AccName == df[5])&(Model3.TrTypeName == df[4])&(Model3.CIS == df[9])]
                if df1.shape[0] == 0:    # same CIS info and no common (TrTypeName, AccName)
                    df1 = Model3[(Model3.TrTypeName == df[4])&(Model3.CIS == df[9] )]
                    if df1.shape[0] == 0:    # same CIS info and no TrTypeName
                        Labels = -1
                    else:                   # same CIS info, with TrTypeName (different AccName)
                        df2 = df1.groupby(['Labels']).size().reset_index()
                        df2.columns = ['Labels', 'num']
                        df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
                        Labels = df2.loc[0,'Labels']
                else:                       # same CIS info, with  (TrTypeName, AccName)
                    df2 = df1.groupby(['Labels']).size().reset_index()
                    df2.columns = ['Labels', 'num']
                    df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
                    Labels = df2.loc[0,'Labels']
                
            else:
                df1 = df2[(df2.AccName == df[5])&(df2.TrTypeName == df[4])]
                if df1.shape[0] == 0:    # same CIS info and no common (TrTypeName, AccName)
                    df1 = df2[(df2.TrTypeName == df[4])]
                    if df1.shape[0] == 0:    # same CIS info and no TrTypeName
                        Labels = -1
                    else:                   # same CIS info, with TrTypeName (different AccName)
                        df2 = df1.groupby(['Labels']).size().reset_index()
                        df2.columns = ['Labels', 'num']
                        df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
                        Labels = df2.loc[0,'Labels']
                else:                       # same CIS info, with  (TrTypeName, AccName)
                    df2 = df1.groupby(['Labels']).size().reset_index()
                    df2.columns = ['Labels', 'num']
                    df2 = df2.sort_values(['num'],ascending=[False]).reset_index(drop=True)
                    Labels = df2.loc[0,'Labels']
    return Labels




















# =============================================================================
# 
# datetime_object = datetime.strptime(SCD.message_time_utc[1][:26], '%b %d %Y %I:%M%p')
# datetime_object = datetime.strptime(SCD.message_time_utc[1][:22], '%Y-%m-%d %H:%M:%S.%f')
# datetime_object = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')
# 
# datetime.strptime(date_date, "%Y-%m-%d %H:%M:%S.%f")
# date = '2018-08-14 13:09:24.543953+00:00'
# date_time_obj = datetime.strptime(date[:26], '%Y-%m-%d %H:%M:%S.%f')
# 
# now = datetime.now()
# now=str(now)
# now=datetime.strptime(now[:19],'%Y-%m-%d %H:%M:%S')
# 
# df1 = pd.DataFrame(SCD.iloc[0:5,4])
# df1['message_time_utc1'] = df1['message_time_utc'].apply(lambda x: datetime.strptime(x[:19],'%Y-%m-%d %H:%M:%S'))
# #SCD1 = df1.query('Mycol > start1 & Mycol < end1')
# SCD1 = df1[(df1.message_time_utc1> start1 )&(df1.message_time_utc1 < end1)]
# SCD2 = df1[(df1.message_time_utc> start )&(df1.message_time_utc < end)]
# 
# SCD3 = df1[(df1.message_time_utc1> start )&(df1.message_time_utc1 < end)]
# SCD4 = df1[(df1.message_time_utc> start1 )&(df1.message_time_utc < end1)]
# 
# =============================================================================

def GetSimilarTrucks(ModelST, Model3, finalModels, ModelCoef, frames_ST,nMix, TrTypeST):
    similarTr = pd.DataFrame({'MixID':[],'TrTypeID':[],'TrTypeIDInd':[],'mean':[],'std':[]})    # empty DataFrame for similar trucks

    TrTypeST = TrTypeST.sort_values().reset_index(drop=True)
    for TrTypeID0 in TrTypeST:
        MixIDWithoutLabel = ModelST[ModelST.TrTypeID == TrTypeID0].MixID.drop_duplicates().reset_index(drop = True)
        
#        Model_MixIDWithLabel = pd.merge(Model3[Model3.TrTypeID == TrTypeID0], ModelCoef, on = ['Labels'],how='left').loc[:,['MixID', 'TrTypeID', 'Labels','coef1', 'coef2', 'coef3']]   # add coefficients
        
        Model_MixIDWithLabel = pd.merge(Model3[Model3.TrTypeID == TrTypeID0], ModelCoef, on = ['Labels'],how='left').loc[:,['MixID', 'TrTypeID', 'Labels','constant_AC','ps_log_AC', 'ps_linear_AC', 'spd_square_AC', 'spd_linear_AC', 'spd_dividing_AC']]   # add coefficients
        
        MixIDWithLabel    = Model_MixIDWithLabel.MixID.drop_duplicates().reset_index(drop = True)
        
    #    df_MixIDWithoutLabel: (Tr_MixIDWithoutLabel, MixIDWithLabel) with speed change data and model coefficients
        df_MixIDWithoutLabel = Model3[Model3.MixID.isin(MixIDWithoutLabel)] 
        if ((df_MixIDWithoutLabel.shape[0]>0) &(MixIDWithLabel.shape[0] > 0)):   # df_MixIDWithoutLabel may be empty
            Tr_MixIDWithoutLabel = df_MixIDWithoutLabel.TrTypeID.drop_duplicates().reset_index(drop = True)    # Truck under MixIDWithoutLabel
    #        MixTrWithLabel: a DataFrame with (Tri, Mixj), some of the MixTr pairs in MixTrWithLabel may not exist.
            MixTrWithLabel = pd.DataFrame({"TrTypeID":Tr_MixIDWithoutLabel.tolist()*len(MixIDWithLabel), "MixID":sorted(MixIDWithLabel.tolist()*len(Tr_MixIDWithoutLabel))})
            MixTrWithLabel = pd.merge(MixTrWithLabel, finalModels, on = ['MixID','TrTypeID'],how='left') # get the labels and coefficients
            MixTrWithLabel = MixTrWithLabel[MixTrWithLabel.Labels.notnull()]
            # if (Tri, Mixj) does not exsit ==> Labels = nan, assign Labels = -100, and pred_slump = 10000
            if (MixTrWithLabel.shape[0]>0):
                
    #            MixTrWithLabel['Labels']= MixTrWithLabel['Labels'].replace(np.nan, -100)
    #            MixTrWithLabel['coef1'] = MixTrWithLabel['coef1'].replace(np.nan, 10000)
    #            MixTrWithLabel['coef2'] = MixTrWithLabel['coef2'].replace(np.nan, 0)
    #            MixTrWithLabel['coef3'] = MixTrWithLabel['coef3'].replace(np.nan, 0)
                
                MixTrWithLabel = MixTrWithLabel.drop(columns = ['n', 'ModelInd'])
                MixTrWithLabel.rename(columns={"TrTypeID":"TrTypeIDInd"}, inplace=True)
                
                # df_MixTr0WithLabel: (Tr0, MixIDWithLabel) with speed change data and model coefficients
                df_MixTr0WithLabel = pd.merge(frames_ST[(frames_ST.TrTypeID == TrTypeID0)&(frames_ST.MixID.isin(MixIDWithLabel))], Model_MixIDWithLabel, on = ['MixID', 'TrTypeID'],how='inner')  # (35726, 14)
                
                # pred_Lo_0 and pred_Hi_0
    #            df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Lo_0=(df_MixTr0WithLabel['coef1'].values + df_MixTr0WithLabel['coef2'].values * df_MixTr0WithLabel['LoLogP'].values)/(1-df_MixTr0WithLabel['coef3'].values*df_MixTr0WithLabel['loRpm'].values))
    #            df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Hi_0=(df_MixTr0WithLabel['coef1'].values + df_MixTr0WithLabel['coef2'].values * df_MixTr0WithLabel['HiLogP'].values)/(1-df_MixTr0WithLabel['coef3'].values*df_MixTr0WithLabel['hiRpm'].values))
                
                df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Lo_0=(df_MixTr0WithLabel['constant_AC'].values + df_MixTr0WithLabel['ps_log_AC'].values * df_MixTr0WithLabel['LoLogP'].values + df_MixTr0WithLabel['ps_linear_AC'].values * df_MixTr0WithLabel['loPsi'].values + df_MixTr0WithLabel['spd_square_AC'].values * (df_MixTr0WithLabel['loRpm'].values)**2 + df_MixTr0WithLabel['spd_linear_AC'].values * df_MixTr0WithLabel['loRpm'].values)/(1+df_MixTr0WithLabel['spd_dividing_AC'].values*df_MixTr0WithLabel['loRpm'].values))
                df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Hi_0=(df_MixTr0WithLabel['constant_AC'].values + df_MixTr0WithLabel['ps_log_AC'].values * df_MixTr0WithLabel['HiLogP'].values + df_MixTr0WithLabel['ps_linear_AC'].values * df_MixTr0WithLabel['hiPsi'].values + df_MixTr0WithLabel['spd_square_AC'].values * (df_MixTr0WithLabel['hiRpm'].values)**2 + df_MixTr0WithLabel['spd_linear_AC'].values * df_MixTr0WithLabel['hiRpm'].values)/(1+df_MixTr0WithLabel['spd_dividing_AC'].values*df_MixTr0WithLabel['hiRpm'].values))
                
    #            df_MixTr0WithLabel = df_MixTr0WithLabel.drop(columns = ['Labels', 'coef1', 'coef2', 'coef3'])
                df_MixTr0WithLabel = df_MixTr0WithLabel.drop(columns = ['Labels', 'constant_AC','ps_log_AC', 'ps_linear_AC', 'spd_square_AC', 'spd_linear_AC', 'spd_dividing_AC'])
                df_MixTr0WithLabel = pd.merge(df_MixTr0WithLabel, MixTrWithLabel, on=['MixID'], how='inner')
                
                # pred_Lo_i and pred_Hi_i
    #            df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Lo_i=(df_MixTr0WithLabel['coef1'].values + df_MixTr0WithLabel['coef2'].values * df_MixTr0WithLabel['LoLogP'].values)/(1-df_MixTr0WithLabel['coef3'].values*df_MixTr0WithLabel['loRpm'].values))
    #            df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Hi_i=(df_MixTr0WithLabel['coef1'].values + df_MixTr0WithLabel['coef2'].values * df_MixTr0WithLabel['HiLogP'].values)/(1-df_MixTr0WithLabel['coef3'].values*df_MixTr0WithLabel['hiRpm'].values))
                
                df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Lo_i=(df_MixTr0WithLabel['constant_AC'].values + df_MixTr0WithLabel['ps_log_AC'].values * df_MixTr0WithLabel['LoLogP'].values + df_MixTr0WithLabel['ps_linear_AC'].values * df_MixTr0WithLabel['loPsi'].values + df_MixTr0WithLabel['spd_square_AC'].values * (df_MixTr0WithLabel['loRpm'].values)**2 + df_MixTr0WithLabel['spd_linear_AC'].values * df_MixTr0WithLabel['loRpm'].values)/(1+df_MixTr0WithLabel['spd_dividing_AC'].values*df_MixTr0WithLabel['loRpm'].values))
                df_MixTr0WithLabel = df_MixTr0WithLabel.assign(pred_Hi_i=(df_MixTr0WithLabel['constant_AC'].values + df_MixTr0WithLabel['ps_log_AC'].values * df_MixTr0WithLabel['HiLogP'].values + df_MixTr0WithLabel['ps_linear_AC'].values * df_MixTr0WithLabel['hiPsi'].values + df_MixTr0WithLabel['spd_square_AC'].values * (df_MixTr0WithLabel['hiRpm'].values)**2 + df_MixTr0WithLabel['spd_linear_AC'].values * df_MixTr0WithLabel['hiRpm'].values)/(1+df_MixTr0WithLabel['spd_dividing_AC'].values*df_MixTr0WithLabel['hiRpm'].values))
                
                # 
                df_MixTr0WithLabel = df_MixTr0WithLabel[df_MixTr0WithLabel.Labels != -100]
                
                df_MixTr0WithLabel['diff_Lo'] = df_MixTr0WithLabel.pred_Lo_i - df_MixTr0WithLabel.pred_Lo_0
                df_MixTr0WithLabel['diff_Hi'] = df_MixTr0WithLabel.pred_Hi_i - df_MixTr0WithLabel.pred_Hi_0
                
                # 'mean_All','std_All': mean(diff_Lo, diff_Hi); std(diff_Lo, diff_Hi)
                df_temp_1=df_MixTr0WithLabel.groupby(['MixID', 'TrTypeID','TrTypeIDInd']).apply(lambda df: [np.mean(df.diff_Lo.append(df.diff_Hi)),np.std(df.diff_Lo.append(df.diff_Hi))]).apply(pd.Series)
                df_temp_1.columns=['mean_All','std_All']
                # mean(diff_Lo); std(diff_Lo); mean(diff_Hi); std(diff_Hi)
                df_temp_2=df_MixTr0WithLabel.groupby(['MixID', 'TrTypeID','TrTypeIDInd']).agg({'diff_Lo':[np.mean,np.std], 'diff_Hi':[np.mean,np.std]})
                df_temp_2.columns = ['mean_Lo', 'std_Lo','mean_Hi','std_Hi']
                
                df_temp_3 = pd.concat([df_temp_1,df_temp_2], axis = 1)
                df_temp_3=df_temp_3.reset_index()
                
                # at least nMix common mix
                df_temp_4 = df_temp_3.groupby(['TrTypeIDInd']).agg({'TrTypeIDInd':'size'}) \
                           .rename(columns={'TrTypeIDInd':'m'}).reset_index() \
                           .merge(df_temp_3,on = ['TrTypeIDInd'],how="left")
                df_temp_4 = df_temp_4[df_temp_4.m>=nMix]
                
                if df_temp_4.shape[0] > 0:
                    # boxplot to get rid of outliers; calculate mean(diff) and std(diff)
                    # df_st shows the similarity between TrTypeID0 and TrTypeIDInd
                    df_st=similarTrucks(df_temp_4).reset_index()
                    df_st['TrTypeID'] = TrTypeID0
                    
                    df_temp_1=df_MixIDWithoutLabel[['MixID','TrTypeID']].drop_duplicates().reset_index(drop=True)
                    df_temp_1=df_temp_1.rename(columns={'TrTypeID':'TrTypeIDInd'})
                    
                    df_temp_1 = pd.merge(df_temp_1,df_st, on=['TrTypeIDInd'],how='inner')
                    df_temp_1 =df_temp_1.groupby(["MixID"]).apply(lambda x: x.sort_values(["std"], ascending = True)).reset_index(drop=True).drop_duplicates(subset='MixID', keep="first").reset_index(drop=True)
                    similarTr=similarTr.append(df_temp_1)
                
    return similarTr



def read_STInfo(cursor):
    
    sqlStr = '''SELECT * \
                FROM web.slump_test;'''
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameST = pd.DataFrame(data=x)
    
    return frameST

def FindScID(row):
    nrow = row.shape[0]
    ScID = row.ScID.drop_duplicates().dropna()
    if len(ScID) == 0:
        ScID = np.nan
    else:
        if len(ScID) == 1:
            ScID = ScID.tolist()[0]
        else:
            ScID = np.nan
            print (row.Mix.drop_duplicates())
            
    return ScID

def CalPred(df, Lo, Hi, append = ''):
    df[Lo+append]=(df['constant'+append].values + df['ps_log'+append].values * df['LoLogP'].values+df['ps_linear'+append].values * df['loPsi'].values+df['spd_square'+append].values * (df['loRpm'].values)**2+df['spd_linear'+append].values * df['loRpm'].values)/(1+df['spd_dividing'+append].values*df['loRpm'].values)
    df[Hi+append]=(df['constant'+append].values + df['ps_log'+append].values * df['HiLogP'].values+df['ps_linear'+append].values * df['hiPsi'].values+df['spd_square'+append].values * (df['hiRpm'].values)**2+df['spd_linear'+append].values * df['hiRpm'].values)/(1+df['spd_dividing'+append].values*df['hiRpm'].values)
#    df = df.assign(pred_Lo_2=(df['constant'+append].values + df['ps_log'+append].values * df['LoLogP'].values+df['ps_linear'+append].values * df['loPsi'].values+df['spd_square'+append].values * (df['loRpm'].values)**2+df['spd_linear'+append].values * df['loRpm'].values)/(1+df['spd_dividing'+append].values*df['loRpm'].values))
#    df = df.assign(pred_Hi_2=(df['constant'+append].values + df['ps_log'+append].values * df['HiLogP'].values+df['ps_linear'+append].values * df['hiPsi'].values+df['spd_square'+append].values * (df['hiRpm'].values)**2+df['spd_linear'+append].values * df['hiRpm'].values)/(1+df['spd_dividing'+append].values*df['hiRpm'].values))
    return df

def addInd(df):
    df = df.reset_index()
    df = df.rename(columns ={'index':'subInd'})
    return df
    
def read_MixCodeInfoMore(cursor):
    
    sqlStr = '''SELECT id,version,name,account_id,description,is_favorite,is_archived,slump_curve_id,audit_trail_created,compute_slump_at_high_rpm_ordinal,system_generated \
                FROM web.mix_code;'''

    colNames=[        'MixID', 'version','Mix', 'AccID',  'description', 'is_favorite', 'Archived','ScID',           'audit_trail_created', 'EnableHighSpeedSlump','system_generated']
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameMix = pd.DataFrame(data=x, columns=colNames)
    
    return frameMix    

def read_MixCodeInfoMoreV(cursor,accID):
    
    sqlStr = '''SELECT id,version,name,account_id,description,is_favorite,is_archived,slump_curve_id,audit_trail_created,compute_slump_at_high_rpm_ordinal,system_generated \
                FROM web.mix_code where account_id = %s;'''

    colNames=[        'MixID', 'version','Mix', 'AccID',  'description', 'is_favorite', 'Archived','ScID',           'audit_trail_created', 'EnableHighSpeedSlump','system_generated']
    
    cursor.execute(sqlStr % str(accID))
    x = cursor.fetchall()
    frameMix = pd.DataFrame(data=x, columns=colNames)
    
    return frameMix    
    
def read_SCCInfoMore(cursor):
    
    sqlStr = '''SELECT id,      slump_curve_id, truck_type_id, do_not_measure, \
                       constant,   ps_log,    ps_linear,      spd_square,    spd_linear,   spd_dividing,       audit_trail_created,  name\
                FROM web.slump_curve_coefficients;'''

    colNames=[        "SccID", "ScID",         "TrTypeID",    "DNM", 
                      "constant", "ps_log",  "ps_linear",    "spd_square",  "spd_linear", "spd_dividing",   "audit_trail_created", "SccName" ]
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameSCC = pd.DataFrame(data=x, columns=colNames)
    
    return frameSCC


def read_TruckTypeInfoMore(cursor):
    
    sqlStr = '''SELECT id,      account_id, name, audit_trail_created\
                FROM web.truck_type;'''

    colNames=[        "TrTypeID", "AccID",         "TrTypeName",    "audit_trail_created" ]
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameTruckType = pd.DataFrame(data=x, columns=colNames)
    
    return frameTruckType

def updateICM(frame_acsc, frames_copy, df_scc, frameSCCmore):
    # add ACM coefficients 
    frames_copy1 = pd.merge(frames_copy, frame_acsc, on = ['MixID', 'TrTypeID'], how = 'inner')
    
    # SccName is not NULL
    frames_copy1 = frames_copy1[~(frames_copy1.SccName.isna())]
    
    # keep AC2 models
    frames_copy1['ACSC'] = frames_copy1.SccName.str.split(' ',expand = True)[0]
    frames_copy1 = frames_copy1[frames_copy1.ACSC == 'ACM']
    
    # n: number of SCD for each (mix, truck) pair; >=12
    frames_copy2 = frames_copy1.groupby(['MixID','TrTypeID']).agg({'MixID':'size'}).rename(columns = {'MixID':'n'}).reset_index()
    frames_copy2 = frames_copy2[frames_copy2.n >= 12]
    frames_copy1 = pd.merge(frames_copy1, frames_copy2, on = ['MixID','TrTypeID'], how = 'inner')
    
    # m: number of SCD for each ('TrTypeID','ScID') pair; >=50
    frames_copy2 = frames_copy1[['MixID','TrTypeID','ScID','n']].drop_duplicates()
    frames_copy3 = frames_copy2.groupby(['TrTypeID','ScID']).apply(lambda x: np.sum(x.n)).reset_index()
    frames_copy3.columns = ['TrTypeID', 'ScID', 'm']
    frames_copy3 = frames_copy3[frames_copy3.m >= 50]
    
    # calculate pred_lo and pred_hi
    frames_copy1 = pd.merge(frames_copy1, frames_copy3, on = ['TrTypeID', 'ScID'], how = 'inner')
    frames_copy1 = CalPred(frames_copy1, 'pred_Lo', 'pred_Hi', append = '')
    frames_copy1 = frames_copy1.rename(columns = {'pred_Lo':'lowSlump'})
    
    # regression model for each ('TrTypeID','ScID') pair
    frames_copy2 = frames_copy1.groupby(['TrTypeID', 'AccID', 'ScID']).apply(slumpModels).reset_index()
    
    frames_copy2.columns=['TrTypeID', 'AccID', 'ScID','All']
    frames_copy2[['coef1','coef2','coef3']] = frames_copy2['All'].apply(pd.Series)
    frames_copy2 = frames_copy2[['TrTypeID', 'AccID', 'ScID', 'coef1', 'coef2', 'coef3']]
    
    frames_copy2['constant_AC']     = frames_copy2.coef1
    frames_copy2['ps_log_AC']       = frames_copy2.coef2
    frames_copy2['ps_linear_AC']    = 0
    frames_copy2['spd_square_AC']   = 0
    frames_copy2['spd_linear_AC']   = 0
    frames_copy2['spd_dividing_AC'] = frames_copy2.coef3
    
    # constant >= 30
    frames_copy2 = frames_copy2[frames_copy2.constant_AC >= 30]
    
    df_IC = frames_copy2.copy()
    df_IC.columns = ['TrTypeID', 'AccID', 'ScID', 'coef1', 'coef2', 'coef3', 'constant','ps_log', 'ps_linear', 'spd_square', 'spd_linear','spd_dividing']
    ## update ICM
    df_IC = pd.merge(df_IC[[ u'constant',       u'ps_log',
              u'ps_linear',   u'spd_square',   u'spd_linear', u'spd_dividing',
               u'TrTypeID',         u'ScID']], frameSCCmore[[u'SccID', u'ScID', u'TrTypeID', u'DNM', 
           u'audit_trail_created', u'SccName']], on = ['TrTypeID',         u'ScID'],how = 'inner')
    
    # DNM = False
    df_IC['DNM'] = False
    
    df_IC = pd.merge(df_IC, df_scc, on = ['ScID', 'TrTypeID'], how = 'inner')
    
    return df_IC

def appFunctionSPD(df):
#    df1 = df[df.loRpm <= 3]
    df1 = df
    if df1.shape[0] >= 10:
        model = LinearRegression(fit_intercept=True).fit(df1.loc[:, ['InterceptL']], df1.loc[:, 'lowSlump'])
        df['pred']=model.predict(df.loc[:, ['InterceptL']])
    else:
        df['pred'] = df.lowSlump
    return df

def slumpCoefficientsSPD(framesT):
    
    ModelCoef = framesT.groupby(['Labels']).apply(slumpModels).reset_index()
    
    ModelCoef.columns=['Labels','All']
    
    ModelCoef[['coef1','coef2','coef3']] = ModelCoef['All'].apply(pd.Series)
    ModelCoef = ModelCoef.drop(columns=['All'])
    
    ModelCoef['constant_AC']     = ModelCoef.coef1
    ModelCoef['ps_log_AC']       = ModelCoef.coef2
    ModelCoef['ps_linear_AC']    = 0
    ModelCoef['spd_square_AC']   = 0
    ModelCoef['spd_linear_AC']   = 0
    ModelCoef['spd_dividing_AC'] = ModelCoef.coef3
    
    ModelCoef = ModelCoef.drop(columns=['coef1','coef2','coef3'])
    
    return ModelCoef  

def ACtool_CreatInitialCurves(frame, finalModelsACM, NewCustomerFile, topN = 5, truncateInd = True):
    
    # get the Truck type and AccID for the new customer
    TrTypeSet1 = pd.read_excel(NewCustomerFile, sheet_name = 'TrType')
    customerID = TrTypeSet1.AccID.drop_duplicates().tolist()
    TrTypeSet1 = TrTypeSet1[['TrTypeName']].drop_duplicates()
    
    # Larger TrTypeName set
    TrTypeSet2 = pd.read_excel(NewCustomerFile, sheet_name = 'TrTypeSet')
    
    # MixInfo
    MixInfoAll = pd.read_excel(NewCustomerFile, sheet_name = 'MixInfo')
    
    MixInfoAll['v1']=MixInfoAll.V1.astype(int).astype(str)
    MixInfoAll['v2']=MixInfoAll.V2.astype(int).astype(str)
    MixInfoAll['ScName'] = MixInfoAll.CIS+'_WC'+(MixInfoAll.v1)+'-'+(MixInfoAll.v2)
    
#    MixInfoAll['CIS'] = MixInfoAll.ScName.str.split('_',expand = True)[0]
#    MixInfoAll['V']   = MixInfoAll.ScName.str.split('_').str[-1]
#    MixInfoAll['V']   = MixInfoAll.V.str[2:]
#    MixInfoAll['V1']  = MixInfoAll.V.str.split('-',expand = True)[0]
#    MixInfoAll['V2']  = MixInfoAll.V.str.split('-',expand = True)[1]
    
    MixInfoAll = MixInfoAll[['Mix','ScName','AccID','CIS','V1','V2']]
    
    # add CIS-WC to frame
    frame = frame[~(frame.AccID.isin(customerID))]
    frame = pd.merge(frame, MixInfoAll, on = ['Mix','AccID'], how = 'inner')
    
    # add ACM to frame
    frame = pd.merge(frame, finalModelsACM, on = ['MixID','TrTypeID'], how = 'inner')
    
    frame = CalPred(frame, 'pred_Lo', 'pred_Hi', append = '_AC')
    frame = CalPred(frame, 'pred_Lo', 'pred_Hi', append = '_MC')
    
    if truncateInd:
        frame['pred_Hi_AC'] = np.where(frame.pred_Hi_AC <0, 0, np.where(frame.pred_Hi_AC > 10, 10, frame.pred_Hi_AC))
        frame['pred_Lo_AC'] = np.where(frame.pred_Lo_AC <0, 0, np.where(frame.pred_Lo_AC > 10, 10, frame.pred_Lo_AC))
        frame['pred_Hi_MC'] = np.where(frame.pred_Hi_MC <0, 0, np.where(frame.pred_Hi_MC > 10, 10, frame.pred_Hi_MC))
        frame['pred_Lo_MC'] = np.where(frame.pred_Lo_MC <0, 0, np.where(frame.pred_Lo_MC > 10, 10, frame.pred_Lo_MC))
    
    # all ACM: frame['lowSlump'] = frame.pred_Lo_AC
    frame['lowSlump'] = np.where(frame.pred_Lo_AC.isna(), frame.pred_Lo_MC,frame.pred_Lo_AC)
    
    MixInfoNew = MixInfoAll[MixInfoAll.AccID.isin(customerID)]
    MixInfoAll = MixInfoAll[~(MixInfoAll.AccID.isin(customerID))]
    
    MixInfoNew = MixInfoNew.drop_duplicates(['ScName']).sort_values('ScName').reset_index(drop=True)
    
    if MixInfoNew.shape[0]==0:
        MixInfoNew = pd.read_excel(NewCustomerFile, sheet_name = 'family')
    # check if it is a brand new truck type
    currentTrType = frame[['TrTypeID','TrTypeName']].drop_duplicates().reset_index(drop = True)
    TrTypeSet1_newTr = pd.merge(TrTypeSet1, currentTrType, on = ['TrTypeName'], how = 'left')
    TrTypeSet1_newTr = TrTypeSet1_newTr.drop_duplicates(['TrTypeName']).reset_index(drop = True)
    
    InitialCurve = pd.DataFrame(data = {'constant':[],'ps_log':[],  'ps_linear':[],   'spd_square':[],'spd_linear':[], 'spd_dividing':[],'ScName':[],'TrTypeName':[]})
    # for each TrTypeName
    for i in np.arange(TrTypeSet1_newTr.shape[0]):
        trTypeName = TrTypeSet1_newTr.loc[i,'TrTypeName']
        if np.isnan(TrTypeSet1_newTr.loc[i,'TrTypeID']):
            print("Brand new truck type: " + trTypeName)
            
            # for each CIS-WC
            for j in np.arange(MixInfoNew.shape[0]):
                print("i="+str(i)+"; j="+str(j))
                mixInfoNew = MixInfoNew.loc[j, 'ScName']
                cis = MixInfoNew.loc[j, 'CIS']
                v1  = MixInfoNew.loc[j, 'V1']
                v2  = MixInfoNew.loc[j, 'V2']
                
                # try set2 first:
                df1 = frame[frame.TrTypeName.isin(TrTypeSet2[TrTypeSet2.TrTypeName==trTypeName].potentialTrType)]
                initialCurve = IC_Steps(df1, cis, v1,v2, topN)
                
                # use all data if we can't get a model
                if initialCurve.shape[0] ==0:
                    initialCurve = IC_Steps(frame, cis, v1,v2, topN)
                    
                    # use default curve if we can't get a model
                    if initialCurve.shape[0] ==0:
                        initialCurve = pd.DataFrame(data = {'constant':[38.2],'ps_log':[-4.96],  'ps_linear':[0],   'spd_square':[0],'spd_linear':[0], 'spd_dividing':[-0.035]})
                                        
                initialCurve['ScName']=mixInfoNew
                initialCurve['TrTypeName']=trTypeName
                
                InitialCurve = InitialCurve.append(initialCurve)
        
        else: 
            # it is not brand new truck: Try set1 first
            for j in np.arange(MixInfoNew.shape[0]):
                print("i="+str(i)+"; j="+str(j))
                mixInfoNew = MixInfoNew.loc[j, 'ScName']
                cis = MixInfoNew.loc[j, 'CIS']
                v1  = MixInfoNew.loc[j, 'V1']
                v2  = MixInfoNew.loc[j, 'V2']
                df1 = frame[frame.TrTypeName==trTypeName]
                initialCurve = IC_Steps(df1, cis, v1,v2, topN)
                
                # try set2
                if initialCurve.shape[0] ==0:
                    df1 = frame[frame.TrTypeName.isin(TrTypeSet2[TrTypeSet2.TrTypeName==trTypeName].potentialTrType)]
                    initialCurve = IC_Steps(df1, cis, v1,v2, topN)
                    
                    # use default curve if we can't get a model
                    if initialCurve.shape[0] ==0:
                        initialCurve = pd.DataFrame(data = {'constant':[38.2],'ps_log':[-4.96],  'ps_linear':[0],   'spd_square':[0],'spd_linear':[0], 'spd_dividing':[-0.035]})
                        
                initialCurve['ScName']=mixInfoNew
                initialCurve['TrTypeName']=trTypeName
                
                InitialCurve = InitialCurve.append(initialCurve)
    
    return InitialCurve

def IC_Steps(df, cis, v1,v2, topN):
    initialCurve = pd.DataFrame(data = {'constant':[],'ps_log':[],  'ps_linear':[],   'spd_square':[],'spd_linear':[], 'spd_dividing':[]})
    df1 = df[(df.CIS == cis)&(df.V1 >= v1)&(df.V2 <= v2)]
    initialCurve = IC_models(df1, topN)
    
    if initialCurve.shape[0] ==0:
        df1 = df[(df.CIS == cis)&(df.V1 >= v1-5)&(df.V2 <= v2+5)]
        initialCurve = IC_models(df1, topN)
        
        if initialCurve.shape[0] ==0:
            df1 = df[(df.CIS == cis)&(df.V1 >= v1-10)&(df.V2 <= v2+10)]
            initialCurve = IC_models(df1, topN)
            
    return initialCurve

def IC_models(df, topN=5):    
    initialCurve = pd.DataFrame(data = {'constant':[],'ps_log':[],  'ps_linear':[],   'spd_square':[],'spd_linear':[], 'spd_dividing':[]})
    
    # each customer at most contains three truck types
    df1 = df[['TrTypeName','TrTypeID','MixID','AccName','n']].drop_duplicates()
    df1 = df1.sort_values(['AccName','n'],ascending = [False,False]).reset_index(drop=True)
    
    df1 = df1.groupby(['AccName']).apply(addInd)
    
    if df1.shape[0] > 0:
        df1 = df1.drop(columns=['subInd','AccName'])
        df1 = df1.reset_index()
        df1 = df1.rename(columns={'level_1':'sub_ind'})
        
        # only keep 3 (Mix, Tr) for each customer
        df1 = df1[df1.sub_ind < 3]
        df1 = df1[['TrTypeID', 'MixID']]
        # merge with df
        df2 = pd.merge(df1,df, on = ['TrTypeID', 'MixID'], how = 'inner')
        
        # apply filters-------
        ########## 38.2, comment out this line
        df2 = df2[(df2.hiRpm >= 12)&(df2.loRpm <= 4)]
        if df2.shape[0] > 10:
            # the numer of data points for training
            df3 = df2.groupby(['TrTypeID', 'MixID']).agg({'MixID':np.size})
            df3 = df3.rename(columns = {'MixID':'m'})
            df3 = df3.reset_index()
            df3 = df3[df3.m > 10]
            df3 = df3.sort_values('m',ascending = False).reset_index(drop=True).reset_index()
            
            df3 = df3[df3.index<topN]
            # 
            df4 = pd.merge(df3[['TrTypeID', 'MixID']], df2, on=['TrTypeID', 'MixID'], how = 'inner')
            
            df4['Labels']=1
            
            if df4.shape[0] > 10:
                df4 = df4.groupby(['Labels']).apply(appFunctionSPD)
                df4['lowSlump'] = df4.pred
            
                ModelCoef_spd_adj = slumpCoefficientsSPD(df4)
                initialCurve = pd.DataFrame(data = {'constant':[ModelCoef_spd_adj.constant_AC[0]],'ps_log':[ModelCoef_spd_adj.ps_log_AC[0]],  'ps_linear':[0],   'spd_square':[0],'spd_linear':[0], 'spd_dividing':[ModelCoef_spd_adj.spd_dividing_AC[0]]})
            initialCurve = initialCurve[['constant', u'ps_log', u'ps_linear','spd_square', u'spd_linear', u'spd_dividing']]

            if initialCurve.shape[0]>0:
                if initialCurve.constant[0]<30:
                    initialCurve['constant']  = 38.2
                    initialCurve['ps_log']  = -4.96
                    initialCurve['spd_dividing']  = -0.035
                
    return initialCurve


def add_date_index(df):
    df1 = df.sort_values(['TimeUTC'], ascending = False)
    df1 = df1.reset_index(drop = True)
    df1['date_index']=df1.index
    
    return df1

def read_ACSCInfoMore(cursor):
    
    sqlStr = '''SELECT id, account_id, mix_code_id, truck_type_id, slump_curve_coefficients_id\
                FROM web.auto_calibration_slump_coefficients;'''

    colNames=[       "ACSCID", "AccID",    "MixID",     "TrTypeID",    "SccID"]
    
    cursor.execute(sqlStr)
    x = cursor.fetchall()
    frameACSC = pd.DataFrame(data=x, columns=colNames)
    
    return frameACSC

def adapt_numpy_int64(numpy_int64):
    """ Adapting numpy.int64 type to SQL-conform int type using psycopg extension, see [1]_ for more info.
    References
    ----------
    .. [1] http://initd.org/psycopg/docs/advanced.html#adapting-new-python-types-to-sql-syntax
    """
    return AsIs(numpy_int64)




