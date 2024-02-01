import warnings                 # для того чтобы библиотеки не выдавали Предупреждение об Устаревании
warnings.filterwarnings("ignore", category=DeprecationWarning)
# import warnings ; warnings.warn = lambda *args,**kwargs: None # альтернативный вариант
import pandas as pd             # Работа с DataFrame
from datetime import datetime   # Работа с Временем

class Logging:
    marks = {
        'debug': False,
        'info': True,
        'warning': True,
        'client table': True
        }
    div_line = '-' * 120

    def __init__(self, message, mark: str = 'info'):
        self.dt = self.get_datetime()
        self.mark = f'{mark.upper()}:'
        self.message = message
        # Если поместить перед атрибутами, то в случ. обращения к ним у непечатаемомого экз. будет Ошибка
        # Хотя размещение в начале позволит в общем ускорить обработку тк послед операции не будут выполняться.
        if self.is_print_mark(mark):
            self.print_log()

    def print_log(self):
        separator = '\n'
        if isinstance(self.message, (tuple)):
            total_message = (f'{self.dt} | {self.mark} | {self.message[0]}', *self.message[1:], self.div_line)
        elif isinstance(self.message, pd.DataFrame):
            total_message = (self.mark, self.message, self.div_line)
        else:
            total_message = (self.mark, self.message)
            separator = ' '
        print(*total_message, sep=separator)

    def get_datetime(self):
        dt_now = datetime.now()
        dt_str = dt_now.strftime('%Y-%m-%d %H:%M:%S')
        return dt_str

    def is_print_mark(self, mark: str):
        try:
            return self.marks[mark]
        except:
            print(f"НЕ Существует Метки Логирования: | {mark} | Информация НЕ отображена")
            return False

def create_df():
    columns = ('column1', 'column2', 'column3')
    data = (['Name1', 44, 47], ['Name2', 54, 43], ['Name3', 24, 87])
    df = pd.DataFrame(data=data, columns=columns)
    return df

if __name__ == '__main__':

    # Тестирование

    df = create_df()

    log1 = Logging('Hello', mark='info')
    log2 = Logging(56778, mark='info')
    log3 = Logging('По умолчанию')
    log4 = Logging(('По умолчанию', 'rtrtr'))
    log5 = Logging(df)
    log6 = Logging(('Client Name', df), mark='client table')
    log7 = Logging(['fgfgfgfg', 55, 'fgffgf'], mark='ytyt')

    print(log6.mark)
    print(log6.message)
    print(log6.dt)




