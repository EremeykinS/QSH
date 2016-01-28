from qsh import *

# qsh = open("OrdLog.RTS-9.14.2014-09-05.qsh", mode='rb')
qsh = open("OrdLog.BR-4.15.2015-02-25.qsh", mode='rb')
# read File Title:
ft = FileTitle(qsh)
print(ft)
# read Thread Title:
tht = ThreadTitle(qsh)
print(tht)
# read Frames
# First
fr = [Frame(ft)]
# 2 more
for i in range(2):
    fr.append(Frame(qsh, fr[-1]))

for frame in fr:
    print(frame)
