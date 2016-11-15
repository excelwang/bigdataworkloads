%matplotlib inline
import datetime
import monetdb.sql
import warnings
import numpy as np
import pandas as pd
import scipy.stats as st
import statsmodels as sm
import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams['figure.figsize'] = (16.0, 12.0)
matplotlib.style.use('ggplot')

# Create models from data
def best_fit_distribution(data, bins=200, ax=None):
    """Model data by finding best fit distribution to data"""
    # Get histogram of original data
    y, x = np.histogram(data, bins=bins, normed=True)
    x = (x + np.roll(x, -1))[:-1] / 2.0

    # Distributions to check
    '''
    DISTRIBUTIONS = [        
        st.alpha,st.anglit,st.arcsine,st.beta,st.betaprime,st.bradford,st.burr,st.cauchy,st.chi,st.chi2,st.cosine,
        st.dgamma,st.dweibull,st.erlang,st.expon,st.exponnorm,st.exponweib,st.exponpow,st.f,st.fatiguelife,st.fisk,
        st.foldcauchy,st.foldnorm,st.frechet_r,st.frechet_l,st.genlogistic,st.genpareto,st.gennorm,st.genexpon,
        st.genextreme,st.gausshyper,st.gamma,st.gengamma,st.genhalflogistic,st.gilbrat,st.gompertz,st.gumbel_r,
        st.gumbel_l,st.halfcauchy,st.halflogistic,st.halfnorm,st.halfgennorm,st.hypsecant,st.invgamma,st.invgauss,
        st.invweibull,st.johnsonsb,st.johnsonsu,st.ksone,st.kstwobign,st.laplace,st.levy,st.levy_l,st.levy_stable,
        st.logistic,st.loggamma,st.loglaplace,st.lognorm,st.lomax,st.maxwell,st.mielke,st.nakagami,st.ncx2,st.ncf,
        st.nct,st.norm,st.pareto,st.pearson3,st.powerlaw,st.powerlognorm,st.powernorm,st.rdist,st.reciprocal,
        st.rayleigh,st.rice,st.recipinvgauss,st.semicircular,st.t,st.triang,st.truncexpon,st.truncnorm,st.tukeylambda,
        st.uniform,st.vonmises,st.vonmises_line,st.wald,st.weibull_min,st.weibull_max,st.wrapcauchy
    ]
    '''
    DISTRIBUTIONS = [        
        st.chi,st.chi2,
        #st.dweibull,
        st.expon,st.exponnorm,
        st.logistic,st.lognorm,
        st.norm,st.powerlaw,st.powerlognorm,st.powernorm,
        st.uniform
    ]

    # Best holders
    best_distribution = st.norm
    best_params = (0.0, 1.0)
    best_sse = np.inf

    # Estimate distribution parameters from data
    for distribution in DISTRIBUTIONS:

        # Try to fit the distribution
        try:
            # Ignore warnings from data that can't be fit
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore')

                # fit dist to data
                params = distribution.fit(data)

                # Separate parts of parameters
                arg = params[:-2]
                loc = params[-2]
                scale = params[-1]

                # Calculate fitted PDF and error with fit in distribution
                pdf = distribution.pdf(x, loc=loc, scale=scale, *arg)
                sse = np.sum(np.power(y - pdf, 2.0))

                # if axis pass in add to plot
                try:
                    if ax:
                        pd.Series(pdf, x).plot(ax=ax)
                    end
                except Exception:
                    pass

                # identify if this distribution is better
                if best_sse > sse > 0:
                    best_distribution = distribution
                    best_params = params
                    best_sse = sse

        except Exception:
            pass

    return (best_distribution.name, best_params)

def make_pdf(dist, params, size=10000):
    """Generate distributions's Propbability Distribution Function """

    # Separate parts of parameters
    arg = params[:-2]
    loc = params[-2]
    scale = params[-1]

    # Get sane start and end points of distribution
    start = dist.ppf(0.01, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.01, loc=loc, scale=scale)
    end = dist.ppf(0.99, *arg, loc=loc, scale=scale) if arg else dist.ppf(0.99, loc=loc, scale=scale)

    # Build PDF and turn into pandas Series
    x = np.linspace(start, end, size)
    y = dist.pdf(x, loc=loc, scale=scale, *arg)
    pdf = pd.Series(y, x)

    return pdf

# fitting distribution for operation arrival times
def fittingArrial(statistics,op_num):
    for i in range(op_num):    
        #print statistics[i][0]
        #continue
        arrival_inter=[0]
        for j in range(len(statistics[i][1][1])-1):
            arrival_inter.append(statistics[i][1][1][j+1]-statistics[i][1][1][j])
        data = pd.Series(arrival_inter)
        # Plot for comparison
        plt.figure(figsize=(12,8))
        ax = data.plot(kind='hist', bins=50, normed=True, alpha=0.5, color=plt.rcParams['axes.color_cycle'][1])
        # Save plot limits
        dataYLim = ax.get_ylim()
        # Find best fit distribution
        best_fit_name, best_fir_paramms = best_fit_distribution(data, 200, ax)
        best_dist = getattr(st, best_fit_name)
        # Update plots
        ax.set_ylim(dataYLim)
        ax.set_title(u'Operation top %d: %s\n All Fitted Distributions' %(i+1,statistics[i][0]))
        ax.set_xlabel(u'Interval (s)')
        ax.set_ylabel('Frequency')
        # Make PDF
        pdf = make_pdf(best_dist, best_fir_paramms)
        # Display
        plt.figure(figsize=(12,8))
        ax = pdf.plot(lw=2, label='PDF', legend=True)
        data.plot(kind='hist', bins=50, normed=True, alpha=0.5, label='Data', legend=True, ax=ax)

        param_names = (best_dist.shapes + ', loc, scale').split(', ') if best_dist.shapes else ['loc', 'scale']
        param_str = ', '.join(['{}={:0.2f}'.format(k,v) for k,v in zip(param_names, best_fir_paramms)])
        dist_str = '{}({})'.format(best_fit_name, param_str)
        #print dist_str
        ax.set_title(u'Operation top %d: %s\n%s' %(i+1,statistics[i][0],dist_str))
        ax.set_xlabel(u'Interval (s)')
        ax.set_ylabel('Frequency')
    
connection = monetdb.sql.connect(username="monetdb", password="monetdb", hostname="localhost", database="gwacdb")
cursor = connection.cursor()
cursor.execute("select query,start from sys.querylog_history")
tables_of_interesting=('uniquecatalog18','image3','targets18','associatedsource18')
wheres=[]
froms=[]
arrivals=[]
values=[]
#print cursor.rowcount
for i in range(int(cursor.rowcount)):
	(query,arrival)=cursor.fetchone()
	query=query.strip('\n;')
	q_parts=query.split(' from ')
	if len(q_parts)==1:
		continue
	q_parts=q_parts[1].split(' where ')
 	from_clause=q_parts[0]
	if len(q_parts)==1:
		where_clause=''
	else:
		where_clause=q_parts[1]
	ts=[]
	for t in from_clause.split(','):
		table_name=t.strip().split(' ')[0].replace('sys.','')
		if table_name not in tables_of_interesting:
			continue
		ts.append(t.strip().split(' ')[0].replace('sys.','').strip())
		#print query
		#print t.strip().split(' ')[0].replace('sys.','')
	if len(ts)==0:
		continue
	arrivals.append((arrival-datetime.datetime(1970,1,1)).total_seconds())
	froms.append(ts)
	where_clause=where_clause.split(' group by ')[0].strip()
	ws=[]
	for w in where_clause.split(' and '):
		for _w in w.split(' or '):
			pair=_w.split('=')
			if len(pair)==2:
				if pair[1].isdigit():
					ws.append(pair[0].strip())
					values.append(_w.strip())
					#print values[len(values)-1]
				else:
					ws.append(_w.strip())
			else:
				pair=_w.split('>')
				if len(pair)==2:
					ws.append(pair[0].strip())
					values.append(_w.strip('\n'))
				else:
					pair=_w.split('<')
					if len(pair)==2:
						ws.append(pair[0].strip())
						values.append(_w.strip('\n'))
						#print pair
	wheres.append(ws)

statistics={}
for i in range(len(wheres)):
	op='-'.join(froms[i])+'-'+'-'.join(wheres[i])
	if op in statistics:
		statistics[op][0]+=1
		statistics[op][1].append(arrivals[i])
	else:
		arrs=[arrivals[i]]
		statistics[op]=[1,arrs]
statistics=sorted(statistics.iteritems(), key=lambda d:d[1][0], reverse = True)
#print "top_operations=%s" %statistics

values_ranks={}
for i in range(len(values)):
    if values[i] in values_ranks:
        values_ranks[values[i]]+=1
    else:
        values_ranks[values[i]]=0
vr=sorted(values_ranks.iteritems(), key=lambda d:d[1], reverse = True)
values_ranks={}
for i in range(len(vr)):
    if vr[i]=='':
        continue
    if i==len(vr)-1:
        values_ranks[vr[i][0]]=vr[i][1]
        continue
    p1=vr[i][0].split('>')
    if len(p1)==1:
        p1=p1[0].split('<')
    p2=vr[i+1][0].split('>')
    if len(p2)==1:
        p2=p2[0].split('<')
    if p1[0]==p2[0] and vr[i][1]==vr[i+1][1]:
        if p1[1]<p2[1]:
            v_range="(%s,%s)" %(p1[1],p2[1])
        else:
            v_range="(%s,%s)" %(p2[1],p1[1])
        values_ranks[p1[0]+v_range]=vr[i][1]
        vr[i+1]=''
    else:
        values_ranks[vr[i][0]]=vr[i][1]
values_ranks=sorted(values_ranks.iteritems(), key=lambda d:d[1], reverse = True)

#operation number in the result model
op_num=4
#accessed value number in the result model
value_num=10

#1
fittingArrial(statistics,op_num)
#2
#print "invovled_values=%s" %values_ranks
for p in values_ranks:
    print p[0],p[1]
