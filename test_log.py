import logging
import pandas as pd

# x = input('Введите число: ')
# assert isinstance(x, (int, float)), 'Это не число'
# print(f'Вы ввели Число {x}')
# # assert x == 0
# # print(f'Выввели ноль')

logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG, # INFO DEBUG WARNING ERROR CRITICAL FATAL
    filename = "test_log.log",
    format = "%(asctime)s | %(module)s | %(name)s | %(levelname)s.%(funcName)s:%(lineno)d - %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='UTF-8',
    filemode='w',
    )

# "%(asctime)s - [%(levelname)s] -  %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"

def create_df():
    columns = ('column1', 'column2', 'column3')
    data = (['Name1', 44, 47], ['Name2', 54, 43], ['Name3', 24, 87])
    df = pd.DataFrame(data=data, columns=columns)
    return df

def main():
    message_df = create_df()

    logging.debug('Отладочное Сообщение')
    logging.info('Информационное Сообщение')
    logging.info(message_df)
    logging.warning('Сообщение - Предупреждение')
    logging.error('Сообщение об Ошибке')
    logging.critical('Сообщение о Критической Проблеме')
    logging.fatal('Сообщение: Фатальный Сбой. То же самое что и Критическая Проблема')


if __name__ == '__main__':
    main()


