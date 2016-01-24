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


fr = []
for i in range(1):
    fr.append(Frame(qsh))
    print(fr[i])



# print(ULeb128.read(qsh))
# print(Leb128.read(qsh))
# print(ByteArray.read(qsh,5))
