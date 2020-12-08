
class A:
    a = str

    def __init__(self, a):
        self.a = a
        print('A')


class B(A):
    b = int

    def __init__(self, a, b):
        super().__init__(a)
        self.b = b
        print('B')


class C(B):
    c = list()
    cc = int

    def __init__(self, a, b, c, cc):
        super().__init__(a, b)
        self.c = c
        self.cc = cc
        print('C')


class TestAttr:
    float_attr = float
    dict_attr = dict()

    def __init__(self, float_attr, dict_attr):
        self.float_attr = float_attr
        self.dict_attr = dict_attr


class Test:
    int_attr = int
    str_attr = str
    list_attr = list()
    object_attr = TestAttr

    def __init__(self, int_attr, str_attr, list_attr, object_attr):
        self.int_attr = int_attr
        self.str_attr = str_attr
        self.list_attr = list_attr
        self.object_attr = object_attr
