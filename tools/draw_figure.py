import matplotlib.pyplot as plt
import numpy as np

# Instance Precision with K varying
from matplotlib.backends.backend_pdf import PdfPages

Path = [0.999758585	,0.999710598	,0.999712048	,0.999712462	,0.999540388]
Query = [1	,0.999793687	,0.999796954	,0.999796954	,0.999796954]
Agent = [0.999975481	,0.999975484	,0.999975527	,0.999975533	,0.999975666]
Head = [1	,1	,1	,1	,1]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['1','2','3','4', '5'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('Instance Precision', fontsize=30)
plt.xlabel('Number of rules from each instance', fontsize=30)
plt.title('Instance Precision with K varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.gca().set_ylim(ymax=1.0002)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.show()

# App Precision with K varying
Path = [0.994482759	,0.993718593	,0.993726474	,0.993726474	,0.992471769]
Query = [1	,0.9984	,0.9984	,0.9984	,0.9984]
Agent = [0.999125109	,0.999125109	,0.999125874	,0.999125874	,0.99912892]
Head = [1	,1	,1	,1	,1]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['1','2','3','4', '5'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('App Precision', fontsize=30)
plt.xlabel('Number of rules from each instance', fontsize=30)
plt.title('App Precision with K varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.gca().set_ylim(ymax=1.002)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.show()

# Instance Recall with K varying
Path = [0.204480928	,0.213208246	,0.214282187	,0.214590791	,0.214763609]
Query = [0.058708801	,0.059819775	,0.060782619	,0.060782619	,0.060782619]
Agent = [0.503431675	,0.503493396	,0.504382175	,0.504505617	,0.507270707]
Head = [0.011628194	,0.011899765	,0.012319467	,0.012319467	,0.012319467]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['1','2','3','4', '5'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.title('Instance Recall with K varying', fontsize=30)
plt.ylabel('Instance Recall', fontsize=30)
plt.xlabel('Number of rules from each instance', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.show()

# App Recall with K varying
Path = [0.417004049	,0.457489879	,0.458068248	,0.458068248	,0.457489879]
Query = [0.357432042	,0.360902256	,0.360902256	,0.360902256	,0.360902256]
Agent = [0.660497397	,0.660497397	,0.661075766	,0.661075766	,0.663389242]
Head = [0.041642568	,0.043956044	,0.044534413	,0.044534413	,0.044534413]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['1','2','3','4', '5'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('App Recall', fontsize=30)
plt.xlabel('Number of rules from each instance', fontsize=30)
plt.gca().set_ylim(ymax=0.78)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('App Recall with K varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

############################################################################
# Instance Precision with Score varying


Path = [0.99869549	,0.99881241	,0.998907843	,0.999553023	,0.999540388][::-1]
Query = [0.999807544,	0.999803691,	0.998419909,	0.998385795,	0.98972462][::-1]
Agent = [0.999975511	,0.999975511	,0.999975511	,0.999971313	,1][::-1]
Head = [1	,1	,1	,1	,1][::-1]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('Instance Precision', fontsize=30)
plt.xlabel('Context Quality Score', fontsize=30)
plt.gca().set_ylim(ymax=1.002)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('Instance Precision with Score varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

# App Precision with Score varying
Path = [0.984930032	,0.987804878	,0.99086758	,0.992628993	,0.992471769]
Query = [0.99829932,	0.998296422,	0.996539792,	0.996521739,	0.987826087][::-1]
Agent = [0.999125874	,0.999125874126	,0.999125874126	,0.999115044	,1]
Head = [1	,1	,1	,1	,1]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('App Precision', fontsize=30)
plt.xlabel('Context Quality Score', fontsize=30)
plt.gca().set_ylim(ymax=1.005)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('App Precision with Score varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

# Instance Recall with Score varying
Path = [0.264609307	,0.24916677	,0.237094186	,0.220836934	,0.214763609]
Query = [0.061893593	,0.060782619	,0.0601901	,0.058955684	,0.056326379]
Agent = [0.504048883	,0.504048883	,0.504048883	,0.430292556	,0.00840637]
Head = [0.011628194	,0.010899889	,0.010899889	,0.01036909	,0.010048142]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('Instance Recall', fontsize=30)
plt.xlabel('Context Quality Score', fontsize=30)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('Instance Recall with Score varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

# App Recall with Score varying
Path = [0.529207634	,0.515326778	,0.502024291	,0.467322152	,0.457489879]
Query = [0.361480625	,0.360902256	,0.356853673	,0.350491614	,0.340659341]
Agent = [0.661075766	,0.661075766	,0.661075766339	,0.6529786	,0.018507808]
Head = [0.035280509	,0.041064199	,0.041064199	,0.038172354	,0.035280509]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('App Recall', fontsize=30)
plt.xlabel('Context Quality Score', fontsize=30)
plt.gca().set_ylim(ymax=0.80)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('App Recall with Score varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

############################################################################
# Instance Precision with Support varying
Path = [0.998861499	,0.999279835	,0.999273633	,0.999552473	,0.999540388]
Query = [0.999800598	,0.999796954	,0.998361998	,0.998327759	,0.989161067][::-1]
Agent = [0.999975504005, 0.999975504005, 0.999975528583, 0.999975785752, 0.999975655476]
Head = [1	,1	,1	,1	,1]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('Instance Precision', fontsize=30)
plt.xlabel('Context Support', fontsize=30)
plt.gca().set_ylim(ymax=1.002)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('Instance Precision with Support varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

# App Precision with Support varying
Path = [0.988597491448,	0.990599294947,	0.990543735225,	0.992610837438,	0.992471769]
Query = [0.998402556	,0.9984	,0.996768982	,0.996710526	,0.988255034][::-1]
Agent = [0.999126637555, 0.999131190269, 0.999131190269, 0.999130434783, 0.999125874126]
Head = [1	,1	,1	,1	,1]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('App Precision', fontsize=30)
plt.xlabel('Context Support', fontsize=30)
plt.gca().set_ylim(ymax=1.002)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('App Precision with Support varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

# Instance Recall with Support varying
Path = [0.249092705	,0.239797556	,0.237748426	,0.220565362	,0.214763609]
Query = [0.061893593	,0.060782619	,0.0601901	,0.058955684	,0.056326379]
Agent = [0.502419207505, 0.503913097149, 0.503913097149, 0.509776570794, 0.507048512529][::-1]
Head = [0.011628194	,0.010899889	,0.010899889	,0.01036909	,0.010048142]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('Instance Recall', fontsize=30)
plt.xlabel('Context Support', fontsize=30)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('Instance Recall with Support varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

# App Recall with Support varying
Path = [0.501445922499,	0.487565066512,	0.484673221515,	0.466165413534,	0.457489879]
Query = [0.361480625	,0.360902256	,0.356853673	,0.350491614	,0.340659341]
Agent = [0.661654135338, 0.665124349335, 0.665124349335, 0.664545980335, 0.661075766339]
Head = [0.041642568	,0.041642568	,0.041642568	,0.037593985	,0.035280509]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['0.1','0.3','0.5','0.7', '0.9'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('App Recall', fontsize=30)
plt.xlabel('Context Support', fontsize=30)
plt.gca().set_ylim(ymax=0.80)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4, fontsize=30)
plt.title('App Recall with Support varying', fontsize=30)
plt.tick_params(axis='both', which='major', labelsize=25)
plt.show()

############################################

span = [0, 311, 427, 594, 738, 886, 1353]
bound = [2436, 2030, 1539, 949, 554, 417, 0][::-1]
gen = [4800, 4066, 3237, 2040, 1089, 465, 0][::-1]
base = [6099, 4954, 4138, 3306, 2424, 1567, 0][::-1]
x = [0, 1, 2, 3, 4, 5, 6, 7]

xticks = [0, 200, 300,400,500,600,700]
plt.xticks(x, xticks)
plt.plot(bound, marker='o')
plt.plot(gen, marker='s')
plt.plot(base, marker='d')
plt.plot(span, marker='p')

plt.ylabel('Running Time(s)', fontsize=20)
plt.xlabel('Number of HTTP packets (thousand)', fontsize=20)
plt.tick_params(axis='both', which='major', labelsize=20)
#plt.gca().set_ylim(ymax=0.75)
plt.legend(['RuleMiner-3', 'RuleMiner-2', 'RuleMiner-1', 'ContextSpan-1'], loc='upper right', ncol=4, fontsize=20)
plt.show()

#############################################
N = 4
ourMeans = (0.9959, 0.7189, 0.9980, 0.6098)
ourStd = (0.00046, 0.01722, 0.00046, 0.01967)
opacity = 0.4

ind = np.arange(N)  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(ind, ourMeans, width, color='b',  yerr=ourStd, alpha=opacity)

samplesMeans = (0.9961, 0.6643, 0.9990, 0.508)
samplesStd = (0.00036, 0.01322, 0.00026, 0.00967)
rects2 = ax.bar(ind + width, samplesMeans, width, color='r', yerr=samplesStd, alpha=opacity)

# add some text for labels, title and axes ticks
#ax.set_ylabel('Performance')
plt.tick_params(axis='both', which='major', labelsize=30)
#ax.set_title('Performance of our method and SAMPLES on IOS dataset', fontsize=30)
ax.set_xticks(ind + width)
ax.set_xticklabels(('App Precision', 'App Recall', 'Instance Precision', 'Instance Recall'))

ax.legend((rects1[0], rects2[0]), ('Our Method', 'SAMPLES'), fontsize=30)
plt.gca().set_ylim(ymax=1.05)
plt.show()
#############################################
N = 4
ourMeans = (0.9959, 0.7189, 0.9980, 0.6098)
ourStd = (0.00146, 0.02722, 0.00146, 0.02967)
opacity = 0.4

ind = np.arange(N)  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(ind, ourMeans, width, color='b',  yerr=ourStd, alpha=opacity)

samplesMeans = (0.9961, 0.6643, 0.9990, 0.518)
samplesStd = (0.00136, 0.01322, 0.00026, 0.00967)
rects2 = ax.bar(ind + width, samplesMeans, width, color='r', yerr=samplesStd, alpha=opacity)

# add some text for labels, title and axes ticks
#ax.set_ylabel('Performance')
#ax.set_title('Performance of our method and SAMPLES on the Android dataset', fontsize=30)
ax.set_xticks(ind + width)
ax.set_xticklabels(('App Precision', 'App Recall', 'Instance Precision', 'Instance Recall'))
plt.tick_params(axis='both', which='major', labelsize=30)
ax.legend((rects1[0], rects2[0]), ('Our Method', 'SAMPLES'), fontsize=30)
plt.gca().set_ylim(ymax=1.05)
plt.show()

################################################
import matplotlib.pyplot as plt
import numpy as np
N = 4
ourMeans = (0.9959, 0.7189, 0.9980, 0.6098)
ourStd = (0.00046, 0.01722, 0.00046, 0.01967)
opacity = 0.4

ind = np.arange(9, 17, 2)  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(ind, ourMeans, width, color='b',  yerr=ourStd, alpha=opacity)

voteMeans = (0.9944, 0.7260, 0.9936, 0.6240)
voteStd = (0.00076, 0.02222, 0.00086, 0.02967)
rects2 = ax.bar(ind + width, voteMeans, width, color='r', yerr=voteStd, alpha=opacity)

stackingMeans = (0.9841, 0.7173, 0.9838, 0.6178)
stackingStd = (0.00176, 0.02522, 0.00186, 0.03267)
rects3 = ax.bar(ind + width * 2, stackingMeans, width, color='g', yerr=stackingStd, alpha=opacity)

# add some text for labels, title and axes ticks
#ax.set_ylabel('Performance')
#ax.set_title('Performance of different ensemble techniques on the IOS dataset', fontsize=30)
ax.set_xticks(ind + width*1.5)
ax.set_xticklabels(('App Precision', 'App Recall', 'Instance Precision', 'Instance Recall'))

plt.tick_params(axis='both', which='major', labelsize=23)
ax.legend((rects1[0], rects2[0], rects3[0]), ('Pipeline', 'Voting', 'Stacking'), fontsize=30)
plt.gca().set_ylim(ymax=1.05)
plt.show()

################################################
import matplotlib.pyplot as plt
import numpy as np
N = 4
ourMeans = (0.9792, 0.7681, 0.9986, 0.4611)
ourStd = (0.00146, 0.02722, 0.00146, 0.02967)
opacity = 0.4

ind = np.arange(1, 9, 2)  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(ind, ourMeans, width, color='b',  yerr=ourStd, alpha=opacity)

voteMeans = (0.9760, 0.7650, 0.9986, 0.4166)
voteStd = (0.00156, 0.02822, 0.00156, 0.03267)
rects2 = ax.bar(ind + width, voteMeans, width, color='r', yerr=voteStd, alpha=opacity)

stackingMeans = (0.9730 , 0.7690 , 0.9984 , 0.4608)
stackingStd = (0.00156, 0.02922, 0.00156, 0.03367)
rects3 = ax.bar(ind + width * 2, stackingMeans, width, color='g', yerr=stackingStd, alpha=opacity)

# add some text for labels, title and axes ticks
#ax.set_ylabel('Performance')
#ax.set_title('Performance of different ensemble techniques on the Android dataset', fontsize=30)
ax.set_xticks(ind + width*1.5)
ax.set_xticklabels(('App Precision', 'App Recall', 'Instance Precision', 'Instance Recall'))

plt.tick_params(axis='both', which='major', labelsize=23)
ax.legend((rects1[0], rects2[0], rects3[0]), ('Pipeline', 'Voting', 'Stacking'), fontsize=30)
plt.gca().set_ylim(ymax=1.05)
plt.show()


###################################
Instance = [0.999758585	,0.999710598	,0.999712048	,0.999712462	,0.999540388]
App = [0.994482759	,0.993718593	,0.993726474	,0.993726474	,0.992471769]

x = [0, 1, 2, 3, 4]
plt.xticks(x, ['1','2','3','4', '5'])
plt.plot(Path, marker='o')
plt.plot(Query, marker='s')
plt.plot(Agent, marker='p')
plt.plot(Head, marker='d')
plt.ylabel('App Precision', fontsize=20)
plt.xlabel('Number of rules from each instance', fontsize=20)
plt.title('App Precision with K varying', fontsize=20)
plt.tick_params(axis='both', which='major', labelsize=20)
plt.gca().set_ylim(ymax=1.002)
plt.legend(['Path', 'Query', 'Agent', 'Head'], loc='upper right', ncol=4)
plt.show()


###################################
N = 4
headMeans = (1,0.012171337,1,0.046847889)
headStd = (0.000, 0.000922, 0, 0.000967)
opacity = 0.4

ind = np.arange(2, 10, 2)  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(ind, headMeans, width, color='b',  yerr=headStd, alpha=opacity)

agentMeans = (0.999975263,0.499012468,0.999122037,0.658183921)
agentStd = (0.00046, 0.01522, 0.00046, 0.01767)
rects2 = ax.bar(ind + width, agentMeans, width, color='r', yerr=agentStd, alpha=opacity)

pathMeans = (0.995648752,0.265510431,0.994482759,0.417004049)
pathStd = (0.00196, 0.02722, 0.00206, 0.03567)
rects3 = ax.bar(ind + width * 2, pathMeans, width, color='g', yerr=pathStd, alpha=opacity)

queryMeans = (0.999824623,0.070374028,0.998405104,0.362058994)
queryStd = (0.00176, 0.02522, 0.00186, 0.03267)
rects4 = ax.bar(ind + width * 3, queryMeans, width, color='y', yerr=queryStd, alpha=opacity)

# add some text for labels, title and axes ticks
#ax.set_ylabel('Performance')
ax.set_title('Performance of rules from different header fields on the IOS dataset', fontsize=20)
ax.set_xticks(ind + width*2)
ax.set_xticklabels(('Instance Precision', 'Instance Recall', 'App Precision', 'App Recall'))

plt.tick_params(axis='both', which='major', labelsize=20)
ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ('Addition', 'Agent', 'Path', 'Query'), ncol=2, fontsize=20)
plt.gca().set_ylim(ymax=1.05)
plt.show()

###################################
N = 4
headMeans = (
    0.999683911,0.164948454,0.990853659,0.40726817
)
headStd = (0.000, 0.000922, 0, 0.000967)
opacity = 0.4

ind = np.arange(2, 10, 2)  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(ind, headMeans, width, color='b',  yerr=headStd, alpha=opacity)

agentMeans = (0.998192408,0.086403227,0.980769231,0.191729323)
agentStd = (0.00126, 0.02522, 0.00126, 0.02767)
rects2 = ax.bar(ind + width, agentMeans, width, color='r', yerr=agentStd, alpha=opacity)

pathMeans = (0.99928991,0.269119104,0.985875706,0.437343358)
pathStd = (0.00176, 0.03122, 0.00176, 0.03567)
rects3 = ax.bar(ind + width * 2, pathMeans, width, color='g', yerr=pathStd, alpha=opacity)


queryMeans = (0.997384306,0.086177222,0.990228013,0.380952381)
queryStd = (0.00156, 0.02922, 0.00156, 0.03367)
rects4 = ax.bar(ind + width * 3, queryMeans, width, color='y', yerr=queryStd, alpha=opacity)

# add some text for labels, title and axes ticks
#ax.set_ylabel('Performance')
#ax.set_title('Performance of rules from different header fields on the Android dataset', fontsize=20)
ax.set_xticks(ind + width*2)
ax.set_xticklabels(('Instance Precision', 'Instance Recall', 'App Precision', 'App Recall'))

plt.tick_params(axis='both', which='major', labelsize=20)
ax.legend((rects1[0], rects2[0], rects3[0], rects4[0]), ('Addition', 'Agent', 'Path', 'Query'), ncol=2, fontsize=20)
plt.gca().set_ylim(ymax=1.05)
plt.show()

###################################
# Running time of structured dataset
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
span = [0, 0.1, 0.13, 0.15, 0.17, 0.20, 0.23]
gen = [0, 1.1, 1.3, 1.5, 1.8, 2.2, 2.5]
base = [0, 8, 10, 12.4, 14.2, 16.6, 19]
x = [0, 1, 2, 3, 4, 5, 6, 7]

xticks = [0, 200, 300,400,500,600,700]
plt.xticks(x, xticks)
plt.plot(gen, marker='s')
plt.plot(base, marker='d')
plt.plot(span, marker='p')

plt.ylabel('Running Time(s)', fontsize=10)
plt.xlabel('Number of HTTP packets (thousand)', fontsize=10)
plt.tick_params(axis='both', which='major', labelsize=10)
#plt.gca().set_ylim(ymax=0.75)
plt.gca().set_ylim(ymax=22)
plt.legend(['RuleMiner-5', 'RuleMiner-4', 'ContextSpan-2'], loc='upper right', ncol=4, fontsize=10)
fileName = '/Users/congzicun/Documents/research/thesis/figure/runningtime_structure.pdf'
pp = PdfPages(fileName)
plt.savefig(pp, format='pdf')
pp.close()
