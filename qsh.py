'''
Created on 03.01.2016

@author: es
'''
import struct


class byte:
    @staticmethod
    def read(stream):
        return stream.read(1)[0]


class ByteArray:
    @staticmethod
    def read(stream, size):
        return stream.read(size)


class uint16:
    @staticmethod
    def read(stream):
        return int.from_bytes(ByteArray.read(stream, 2), byteorder='big', signed=False)


class uint32:
    @staticmethod
    def read(stream):
        return int.from_bytes(ByteArray.read(stream, 4), byteorder='big', signed=False)


class int64:
    @staticmethod
    def read(stream):
        return int.from_bytes(ByteArray.read(stream, 8), byteorder='big', signed=True)


class double:
    @staticmethod
    def read(stream):
        struct.unpack('d', ByteArray.read(stream, 8))


class ULeb128:
    @staticmethod
    def read(stream):
        value = 0
        shift = 0
        while True:
            b = stream.read(1)[0]
            value |= ((b & 0x7f) << shift)
            if (b & 0x80) == 0:
                break
            shift += 7
        return value


class Leb128:
    @staticmethod
    def read(stream):
        value = 0
        shift = 0
        size = 64  # int64 is 8 bytes
        while True:
            b = stream.read(1)[0]
            value |= ((b & 0x7f) << shift)
            shift += 7
            if (b & 0x80) == 0:
                break
        if (shift < size) and ((b & 0x40) != 0):
            value |= -(1 << shift)
        return value


class DateTime:
    @staticmethod
    def read(stream):
        return int64.read(stream)


class Relative:
    @staticmethod
    def read(stream):
        return Leb128.read(stream)


class String:
    @staticmethod
    def read(stream):
        size = ULeb128.read(stream)
        s = ByteArray.read(stream, size)
        return s.decode('utf-8')


class Growing:
    @staticmethod
    def read(stream):
        result = ULeb128.read(stream)
        if result == 268435455:
            result = Leb128.read(stream)
        return result


class GrowDateTime:
    @staticmethod
    def read(stream):
        return Growing.read(stream)


class FileTitle:
    def __init__(self, stream):
        # set file pointer to the begin of file
        self.sign = ByteArray.read(stream, 19)
        self.version = byte.read(stream)
        self.application = String.read(stream)
        self.comment = String.read(stream)
        self.start = DateTime.read(stream)
        self.n_threads = byte.read(stream)

    def __str__(self):
        r = self.sign.decode('utf-8')
        r += ' File\n'
        r += '\t> version: ' + str(self.version) + '\n'
        r += '\t> application: ' + self.application + '\n'
        r += '\t> comments: ' + self.comment + '\n'
        r += '\t> threads: ' + str(self.n_threads) + '\n'
        # no start time here
        return r


class ThreadTitle:
    _n = 0

    def __init__(self, stream):
        self.thread_type = byte.read(stream)
        self.security = String.read(stream)
        self._id = ThreadTitle._n
        ThreadTitle._n += 1

    def __str__(self):
        t = None
        if self.thread_type == 0x10:
            t = 'Stock'
        elif self.thread_type == 0x20:
            t = 'Deals'
        elif self.thread_type == 0x30:
            t = 'Orders'
        elif self.thread_type == 0x40:
            t = 'Trades'
        elif self.thread_type == 0x50:
            t = 'Messages'
        elif self.thread_type == 0x60:
            t = 'AuxInfo'
        elif self.thread_type == 0x70:
            t = 'OrdLog'
        return 'Thread #' + str(self._id) + '\n\t> type: ' + t + '\n\t> Security: ' + self.security


# желательно заменить этот класс на словарь
class OrdLogData:
    def __init__(self, binfo, ex_time=None, ord_no=None, ord_price=None, vol=None, rest=None, deal_no=None,
                 deal_price=None, oi=None):
        (self.NonZeroReplAct,
         self.SessIdChanged,
         self.Add,
         self.Fill,
         self.Buy,
         self.Sell,
         self.reserved,
         self.Quote,
         self.Counter,
         self.NonSystem,
         self.EndOfTransaction,
         self.FillOrKill,
         self.Moved,
         self.Canceled,
         self.CanceledGroup,
         self.CrossTrade) = binfo
        self.ex_time = ex_time
        self.ord_no = ord_no
        self.ord_price = ord_price
        self.vol = vol
        self.rest = rest
        self.deal_no = deal_no
        self.deal_price = deal_price
        self.oi = oi


class FrameTitle:
    _n = 0

    def __init__(self, stream, one_thread=True):
        self.timestamp = GrowDateTime.read(stream)
        if one_thread:
            self.thread_n = 0
        else:
            self.thread_n = byte.read(stream)
        FrameTitle._n += 1

    def __str__(self):
        return 'Frame ' + '(timestamp:' + str(self.timestamp) + '; thread #' + str(self.thread_n) + ')'


class FrameData:
    def __init__(self, stream):
        b = byte.read(stream)
        self.bflags = list(map(lambda x: int(bool(x)), ((b & mask) for mask in (2 ** x for x in range(8)))))  # [::-1]
        b2 = uint16.read(stream)
        info = list(map(lambda x: int(bool(x)), ((b2 & mask) for mask in (2 ** x for x in range(16)))))  # [::-1]
        if info[-3]:
            structure = (GrowDateTime, Growing, Relative, Leb128, Leb128, Growing, Relative, Relative)
        else:
            structure = (GrowDateTime, Relative, Relative, Leb128, Leb128, Growing, Relative, Relative)
        values = dict()  # возможно, стоит использовать defaultdict()
        keys = ('ex_time', 'ord_no', 'ord_price', 'vol', 'rest', 'deal_no', 'deal_price', 'oi')
        for i, v in enumerate(keys):
            if self.bflags[i]:
                # print("reading ", v, " as ", structure[i])
                values[v] = structure[i].read(stream)
            else:
                values[v] = None
        self.data = OrdLogData(info, **values)

    def __str__(self):
        result = ''
        result += '\t> ReplAct: ' + str(self.data.NonZeroReplAct)
        result += '\n\t> Идентификатор сессии изменен:' + str(self.data.SessIdChanged)
        result += '\n\t> Новая заявка: ' + str(self.data.Add)
        result += '\n\t> Заявка сведена в сделку: ' + str(self.data.Fill)
        result += '\n\t> Покупка: ' + str(self.data.Buy)
        result += '\n\t> Продажа: ' + str(self.data.Sell)
        result += '\n\t> Зарезервированное поле (0): ' + str(self.data.reserved)
        result += '\n\t> Котировочная: ' + str(self.data.Quote)
        result += '\n\t> Встречная: ' + str(self.data.Counter)
        result += '\n\t> Внесистемная: ' + str(self.data.NonSystem)
        result += '\n\t> Является последней в транзакции: ' + str(self.data.EndOfTransaction)
        result += '\n\t> Fill-Or-Kill: ' + str(self.data.FillOrKill)
        result += '\n\t> Перемещение: ' + str(self.data.Moved)
        result += '\n\t> Удаление: ' + str(self.data.Canceled)
        result += '\n\t> Групповое удалеление: ' + str(self.data.CanceledGroup)
        result += '\n\t> Удаление остатка по причине кросс-сделки: ' + str(self.data.CrossTrade)
        #
        result += '\n\t> Биржевое время: ' + str(self.data.ex_time)
        result += '\n\t> Номер заявки: ' + str(self.data.ord_no)
        result += '\n\t> Цена в заявке: ' + str(self.data.ord_price)
        result += '\n\t> Объем операции: ' + str(self.data.vol)
        result += '\n\t> Остаток в заявке: ' + str(self.data.rest)
        result += '\n\t> Номер сделки: ' + str(self.data.deal_no)
        result += '\n\t> Цена сделки: ' + str(self.data.deal_price)
        result += '\n\t> Открытый интерес после сделки: ' + str(self.data.oi)
        return result


class Frame:
    def __init__(self, stream):
        self.title = FrameTitle(stream)
        self.data = FrameData(stream)

    def __str__(self):
        return str(self.title) + '\n' + str(self.data)
