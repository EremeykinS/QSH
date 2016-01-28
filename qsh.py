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
    def read(stream, last):
        return last + Leb128.read(stream)


class String:
    @staticmethod
    def read(stream):
        size = ULeb128.read(stream)
        s = ByteArray.read(stream, size)
        return s.decode('utf-8')


class Growing:
    @staticmethod
    def read(stream, last):
        result = ULeb128.read(stream)
        if result == 268435455:
            result = Leb128.read(stream)
        return last + result


class GrowDateTime:
    @staticmethod
    def read(stream, last):
        return Growing.read(stream, last)


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
        r += '\t> start time: ' + str(self.start) + '\n'
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


class FrameTitle:
    _n = 0

    def __init__(self, *args, one_thread=True):
        if len(args) != 2:
            self.timestamp = args[0].start
            self.thread_n = 0
        else:
            stream, prev = args
            self.timestamp = GrowDateTime.read(stream, prev.timestamp)
            if one_thread:
                self.thread_n = 0
            else:
                self.thread_n = byte.read(stream)
            FrameTitle._n += 1

    def __str__(self):
        return 'Frame ' + '(timestamp:' + str(self.timestamp) + '; thread #' + str(self.thread_n) + ')'


class FrameData:

    n = 0

    def __init__(self, *args):
        if len(args) != 2:
            # flags
            self.NonZeroReplAct = 0
            self.SessIdChanged = 0
            self.Add = 0
            self.Fill = 0
            self.Buy = 0
            self.Sell = 0
            # 6th bit is reserved
            self.reserved = 0
            self.Quote = 0
            self.Counter = 0
            self.NonSystem = 0
            self.EndOfTransaction = 0
            self.FillOrKill = 0
            self.Moved = 0
            self.Canceled = 0
            self.CanceledGroup = 0
            self.CrossTrade = 0
            # main data
            self.exchange_time = 0
            self.ord_no = 0
            self.ord_price = 0
            self.vol = 0
            self.rest = 0
            self.deal_no = 0
            self.deal_price = 0
            self.oi = 0
        else:
            stream, prev = args
            FrameData.n += 1
            b1 = byte.read(stream)
            b2 = uint16.read(stream)
            self.NonZeroReplAct = (b2 & 2**0)
            self.SessIdChanged = (b2 & 2**1)
            self.Add = (b2 & 2**2)
            self.Fill = (b2 & 2**3)
            self.Buy = (b2 & 2**4)
            self.Sell = (b2 & 2**5)
            # 6th bit is reserved
            self.reserved = (b2 & 2**6)
            self.Quote = (b2 & 2**7)
            self.Counter = (b2 & 2**8)
            self.NonSystem = (b2 & 2**9)
            self.EndOfTransaction = (b2 & 2**10)
            self.FillOrKill = (b2 & 2**11)
            self.Moved = (b2 & 2**12)
            self.Canceled = (b2 & 2**13)
            self.CanceledGroup = (b2 & 2**14)
            self.CrossTrade = (b2 & 2**15)
            if b1 & 2**0:
                self.exchange_time = GrowDateTime.read(stream, prev.exchange_time)
            if b1 & 2**1:
                if self.Add:
                    self.ord_no = Growing.read(stream, prev.ord_no)
                else:
                    self.ord_no = Relative.read(stream, prev.ord_no)
            if b1 & 2**2:
                self.ord_price = Relative.read(stream, prev.ord_price)
            if b1 & 2**3:
                self.vol = Leb128.read(stream)
            if b1 & 2**4:
                self.rest = Leb128.read(stream)
            if b1 & 2**5:
                self.deal_no = Growing.read(stream, prev.deal_no)
            if b1 & 2**6:
                self.deal_price = Relative.read(stream, prev.deal_price)
            if b1 & 2**7:
                self.oi = Relative.read(stream, prev.oi)

    def __str__(self):
        result = ''
        result += '\t> ReplAct: ' + str(bool(self.NonZeroReplAct))
        result += '\n\t> Идентификатор сессии изменен:' + str(bool(self.SessIdChanged))
        result += '\n\t> Новая заявка: ' + str(bool(self.Add))
        result += '\n\t> Заявка сведена в сделку: ' + str(bool(self.Fill))
        result += '\n\t> Покупка: ' + str(bool(self.Buy))
        result += '\n\t> Продажа: ' + str(bool(self.Sell))
        result += '\n\t> Зарезервированное поле (0): ' + str(self.reserved)
        result += '\n\t> Котировочная: ' + str(bool(self.Quote))
        result += '\n\t> Встречная: ' + str(bool(self.Counter))
        result += '\n\t> Внесистемная: ' + str(bool(self.NonSystem))
        result += '\n\t> Является последней в транзакции: ' + str(bool(self.EndOfTransaction))
        result += '\n\t> Fill-Or-Kill: ' + str(bool(self.FillOrKill))
        result += '\n\t> Перемещение: ' + str(bool(self.Moved))
        result += '\n\t> Удаление: ' + str(bool(self.Canceled))
        result += '\n\t> Групповое удалеление: ' + str(bool(self.CanceledGroup))
        result += '\n\t> Удаление остатка по причине кросс-сделки: ' + str(bool(self.CrossTrade))
        #
        try:
            result += '\n\t> Биржевое время: ' + str(self.exchange_time)
        except AttributeError:
            pass
        try:
            result += '\n\t> Номер заявки: ' + str(self.ord_no)
        except AttributeError:
            pass
        try:
            result += '\n\t> Цена в заявке: ' + str(self.ord_price)
        except AttributeError:
            pass
        try:
            result += '\n\t> Объем операции: ' + str(self.vol)
        except AttributeError:
            pass
        try:
            result += '\n\t> Остаток в заявке: ' + str(self.rest)
        except AttributeError:
            pass
        try:
            result += '\n\t> Номер сделки: ' + str(self.deal_no)
        except AttributeError:
            pass
        try:
            result += '\n\t> Цена сделки: ' + str(self.deal_price)
        except AttributeError:
            pass
        try:
            result += '\n\t> Открытый интерес после сделки: ' + str(self.oi)
        except AttributeError:
            pass
        return result


class Frame:
    def __init__(self, *args):
        if len(args) != 2:
            self.title = FrameTitle(*args)
            self.data = FrameData()
        else:
            stream, prev_frame = args
            self.title = FrameTitle(stream, prev_frame.title)
            self.data = FrameData(stream, prev_frame.data)

    def __str__(self):
        return str(self.title) + '\n' + str(self.data)
