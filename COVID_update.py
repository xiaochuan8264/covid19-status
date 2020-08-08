"""Requirement:
               1. mysql has been installed in computer, and the specific
                  database has been created in mysql, e.g covid_update
               2. libraries requied have been installed in python"""
from bs4 import BeautifulSoup as bf
import requests as rq
import pymysql, time, pickle, openpyxl, os, re, json

__all__ = ['acquire_covid_data','updateMysql','organize_all_tables_into_one']

def access_web(name,url='https://www.worldometers.info/coronavirus/'):
    """access web 'https://www.worldometers.info/coronavirus/'
       and return response object
       params:
            <name>: the file name you want to use for storing the web page acquired,
                    a suffix '_疫情数据.txt' would be added at the end of the name.
            <url>: default to 'https://www.worldometers.info/coronavirus/',
                    can be altered into a different address"""
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36'}
    file_name = name + '_疫情数据.txt'
    print('accessing web: %s'%url)
    response = rq.get(url=url, headers=headers)
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(response.text)
    return response

def country_data(tag):
    """
    return a list "info"
    Make reference to the data structure in the following website:
    --'https://www.worldometers.info/coronavirus/'
    For instance:
    info = ['USA', '1064572', '378', '61669 ', '13', '147411',
            '855492', '18671', '3216', '186', '6139911',
            '18549', 'North America'
            ]
    while the most citical info would be info[0], info[1], info[6] and info[3]
    which are country, total cases, active cases and total death
    """
    info = tag.text.split('\n')
    info.pop(0)
    info.pop()
    return info

def get_target(web):
    """get the target tags which contain the covid information
       of each country and region then return tags
       Note: param <web> has to be in text format or byte format, not textwraper"""
    soup = bf(web, 'lxml')
    tbody = soup.find('tbody')
    trs = tbody.find_all(name='tr', class_=False)
    return trs

def formatdata(statistic):
    """
       this function is meant to:
       1. replace the field where is empty or N/A with default '0';
       2. remove the thounds separator',' in <statistic>
       A new statistic list would be returned after processing
    """
    for i in range(len(statistic)):
        if statistic[i] == '' or statistic[i] == ' ':
            statistic[i] = '0'
        elif statistic[i] == 'N/A':
            statistic[i] = '0'
    statistic = [_.replace('+','').replace(',','') for _ in statistic]
    return statistic

def all_data(trs):
    """
       Pass into 'tr' tags acquired from function (get_target)
       return all statistics containing the info of coronavirus worldwide
       The returned object>>
            statistics: [[covid_data_of_country1],[covid_data_of_country2],
                          [covid_data_of_country3],...[covid_data_of_country_n]]
    """
    statistics = [country_data(i) for i in trs]
    for i in range(len(statistics)):
        statistics[i] = formatdata(statistics[i])
    return statistics

def database_process(today, all_info): # this function is abandoned

    create_table = """CREATE TABLE IF NOT EXISTS %s(
                      id INT NOT NULL AUTO_INCREMENT,
                      country_region VARCHAR(100) NOT NULL,
                      total_cases INT NOT NULL DEFAULT '0' ,
                      new_cases INT NOT NULL DEFAULT '0',
                      total_death INT NOT NULL DEFAULT '0',
                      new_death INT NOT NULL DEFAULT '0',
                      recovered INT NOT NULL DEFAULT '0',
                      active_cases INT NOT NULL DEFAULT '0',
                      serious_cases INT NOT NULL DEFAULT '0',
                      PRIMARY KEY(id));
                      """ % today
    cursor.execute(create_table)
    insert = """INSERT INTO %s(country_region, total_cases,new_cases,total_death,
                new_death, recovered,active_cases, serious_cases) """% today
    for each in all_info:
        values = """VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s");"""% tuple(each)
        sql = insert + values
        cursor.execute(sql)
    print('成功写入 %d 个国家疫情信息' %len(all_info))
    cursor.connection.commit()

def save_data(name, content):
    """
    <name>: the file name you want to use for storing the target <content>,
            a suffix '_疫情数据.txt' would be added at the end of the name.
    <content>: list or dict or set or tuple, in this module normally would be
            a list of covid statistics
    """
    print('save data..')
    temp = pickle.dumps(content)
    file_name = name + '_疫情数据.pl'
    # today = '_'.join([str('%02d'%_) for _ in time.localtime()[:3]])
    with open(file_name,'wb') as f:
        f.write(temp)

class DataBase: # this function is abandoned
    def __init__(self, db, cur):
        self.db = db
        self.cur = cur
        self.tables = self.get_tables(cur)
        self.total_cases_sql = self.select_total_cases(self.tables)
        self.active_cases_sql = self.select_active_cases(self.tables)
        self.data = []
        self.data_combined = []

    @staticmethod
    def get_tables(cur):
        cur.execute('show tables;')
        # temp =
        tables = [_[0] for _ in cur.fetchall()]
        # db.close()
        return tables

    @staticmethod
    def select_total_cases(tables):
        select_ = 'SELECT %s.country_region'%tables[0]
        for i in tables:
            temp = ', %s.total_cases %s'% (i,i)
            select_ += temp
        from_ = ' FROM %s'%tables[0]
        for i in tables[1:]:
            temp = ', %s' % i
            from_ += temp
        where_ = ' WHERE '
        for i in tables[1:]:
            temp = '%s.country_region=%s.country_region '%(tables[0],i)
            where_ += temp
            if tables.index(i) != len(tables) -1:
                where_ += 'and '
        order_ = 'ORDER BY %s.total_cases DESC;'%tables[-1]
        sql = select_ + from_ + where_ + order_
        return sql

    @staticmethod
    def select_active_cases(tables):
        select_ = 'SELECT %s.country_region'%tables[0]
        for i in tables:
            temp = ', %s.active_cases %s'% (i,i)
            select_ += temp
        from_ = ' FROM %s'%tables[0]
        for i in tables[1:]:
            temp = ', %s' % i
            from_ += temp
        where_ = ' WHERE '
        for i in tables[1:]:
            temp = '%s.country_region=%s.country_region '%(tables[0],i)
            where_ += temp
            if tables.index(i) != len(tables) -1:
                where_ += 'and '
        order_ = 'ORDER BY %s.active_cases DESC;'%tables[-1]
        sql = select_ + from_ + where_ + order_
        return sql

    def get_data(self,sql):
        self.data.clear()
        self.cur.execute(sql)
        self.data = self.cur.fetchall()
        self.data = [list(_) for _ in self.data]

    def process(self):
        temp = self.tables.copy()
        temp.insert(0, 'country')
        self.data_combined = self.data
        self.data_combined.insert(0, temp)

class SaveExcel: # this function is abandoned
    def __init__(self):
        self.workbook = 'covid.xlsx'
        self.wb = self.getworkbook()

    @staticmethod
    def getworkbook():
        workbook = 'covid.xlsx'
        if os.path.exists(workbook):
            wb = openpyxl.load_workbook(workbook)
        else:
            wb = openpyxl.Workbook()
        return wb

    def worksheet(self,data, name='data sheet'):
        ws = self.wb.create_sheet(name)
        for i in data:
            ws.append(i)
        self.wb.save(self.workbook)

def main():
    today = '_'.join([str('%02d'%_) for _ in time.localtime()[:3]])
    web = access_web()
    raw_data = get_target(web.text)
    COVID_data = all_data(raw_data)
    save_data(today,COVID_data)
    info_needed = [[_[0],_[1],_[6],_[3]] for _ in COVID_data]
    try:
        database_process(today, info_needed)
    except TypeError as e:
        print('原始数据错误...',e)
    finally:
        return COVID_data # this function is abandoned

def generate_excel():
    db = pymysql.connect('localhost', 'root', 'tyc1234', 'COVID_19')
    cur = db.cursor()
    excel = SaveExcel()
    covid = DataBase(db, cur)
    covid.get_data(covid.total_cases_sql)
    covid.process()
    excel.worksheet(covid.data_combined, 'total_case')
    covid.get_data(covid.active_cases_sql)
    covid.process()
    excel.worksheet(covid.data_combined, 'active_cases')
    covid.db.close()
    excel.wb.close() # this function is abandoned

def all_url(trs):
    """
    INSTRUCTIONS:
            pass into the 'tr' tags acquired from function(get_target)
            the return the url links for all countries which can provide
            detailed covid statistics of each country since the outbreak
            of covid
        return: Links of all countries
    """
    urls = {}
    for each in trs:
        target = each.find(href=True)
        try:
            country = target.text
            urls[country] = 'https://www.worldometers.info/coronavirus/' + target['href']
        except AttributeError as e:
            error = each.td.text
            print('%s has no links'% error)
    return urls

def extract_data(statistics):
    """pass into the target raw data, which should be string.
       return a dict containing the extracted info from passed <statistics>
       info_extracted = {'coronavirus-death-log':{data},
                         'coronavirus-cases-log':{data}，
                         'Number of Infected People':{date}}
       detailed as bellow:
       1、'Number of Infected People':{categories: date, data: numbers}
       2、'coronavirus-death-log':{categories: date, data: numbers}
       3、'coronavirus-cases-log':{categories: date, data: numbers}
       which are all in dict forms as well
       """
    info_extracted = {}
    for k, v in statistics.items():
        pattern1 = re.compile('categories.+?\]')
        categories = pattern1.search(v).group()
        categories = json.loads(categories.split(':')[1])
        # date = [_ + ' 2020' for _ in date]
        categories = [formatdate(_ + ' 2020') for _ in categories]
        pattern2 = re.compile('data.+?\]')
        data = pattern2.search(v).group()
        data = json.loads(data.split(':')[1])
        info_extracted[k] = {'categories': categories, 'data': data}
    return info_extracted

def formatdate(single_date):
    """change date format like
       'May 03 2020'
       INTO
       '2020_05_03'
       """
    temp = time.strptime(single_date,'%b %d %Y')
    return time.strftime('%Y_%m_%d',temp)

def find_data(page):
    """pass into the text web page of COVID19 of a certain country,
       return a dict containing 3 segments:
       1、'Number of Infected People': strings
       2、'coronavirus-death-log': strings
       3、'coronavirus-cases-log': strings
       the returned data are all raw data, in other words, strings
       which require futher processing.
       """
    pattern = re.compile('Highcharts\.chart\([\s\S]*?;')
    data = pattern.findall(page)
    targets = ['Number of Infected People','coronavirus-death-log','coronavirus-cases-log']
    final = {}
    for each in targets:
        for info in data:
            try:
                temp = re.search(each, info).group()
                final[each] = info
            except AttributeError as e:
                pass
    return final

def re_organizeData(data):
    """
    the original <data> structure would be like below, e.g.:
        data = [('America', {'coronavirus-death-log':{data},
                             'coronavirus-cases-log':{data}，
                             'Number of Infected People':{date}
                             }),
                ('Spain',{'coronavirus-death-log':{data},
                         'coronavirus-cases-log':{data}，
                         'Number of Infected People':{date}
                             }),
                ('other country', {dict data}), , , ,
                            ]
    the data to be return after this funtion,e.g.:
        data = {'2020_01_22': [a list of countries with covid statistics],
                '2020_01_23': [a list of countries with covid statistics],
                , , , ,
                '2020_05_03':[a list of countries with covid statistics],
                , , , continue}
    """

    def extract_single(day, statistics):
        """
        this funtion is to extract single item of each packed
        covid info of a single country of a specific date
        (note that there are 3 items packed together),
        e.g. 'coronavirus-cases-log'
        params:
              day: a date to match the data in statistics
              statistics: one of the packed covid items of a certain country
                          e.g.'coronavirus-cases-log'
        return: the data of a certain catagory from a country on a specific date
                meaning e.g >> on 2020_05_03, the number of death in America is 50,000
        """
        date_range = statistics['categories']
        num_range = statistics['data']
        position = date_range.index(day)
        return num_range[position]

    def organize_single_country(one_country):
        """return structured covid info of the given country
           accoring to date.
           [a, b, c]
           a is total cases
           b is active cases
           c is total death
           --------------
           the final return value would be:
           {'given_country': {'date1':[a,b,c]},
                              'date2':[a,b,c],
                              ,....,
                              'date_n':[a,b,c]}
           """
        nation = one_country[0]
        days = one_country[1]['coronavirus-cases-log']['categories']
        covidInfo = {nation:{}}
        for day in days:
            total_cases = extract_single(day, one_country[1]['coronavirus-cases-log'])
            active_cases = extract_single(day,one_country[1]['Number of Infected People'])
            try:
                total_death = extract_single(day,one_country[1]['coronavirus-death-log'])
            except KeyError as e:
                total_death = 0
            covidInfo[nation][day] = [total_cases, active_cases, total_death]
        return covidInfo

    def fetch(day):
        """
        # build a dict container for storing covid data of all countries according to date
        # in other words, date is key, date is value
        # the return value would be,e.g:
            {'2020_05_03':[['America',1152379, 915269, 66929],
                           ['Spain',245567,74234,25100],
                           ,...,
                           ['Portugal',25190,22496,1023]}
        the date in the example may vary
        """
        def sort_criteria(element):
            return element[1]
        # build a dict container for storing covid data of all countries according to date
        # in other words, date is key, date is value
        covid_at_cur_day = {day:[]}
        # get a list of countries
        countries = covidInfo_allCountries.keys()
        for country in countries:
            try:
                data = covidInfo_allCountries[country][day]
            except KeyError as e:
                # if a country's covid data on a specific date is none
                # default it into 0
                data = [0, 0, 0]
            finally:
                data.insert(0, country)
                covid_at_cur_day[day].append(data)
        covid_at_cur_day[day].sort(key=sort_criteria, reverse=True)
        return covid_at_cur_day

    covidInfo_allCountries = {}
    # this process would re-concstruct data structure of all countries
    for country in  data:
        try:
            covidInfo = organize_single_country(country)
            covidInfo_allCountries.update(covidInfo)
        except KeyError as e:
            e = str(e)
            print(country[0]+ ' >>> ' + e)
    China = [i[1] for i in data if i[0]=='China'][0]
    days = China['coronavirus-cases-log']['categories'] # because China is the first country identified covid
    days_data = {}
    for day in days:
        cur_day_data = fetch(day)
        days_data.update(cur_day_data)
    return days_data

def dump_in(tablename, tabledata):
    """
        Requirements:
                     1. connection with database has been set up
                     2. cur has been defined for executing sql
        parmas:
               tablename: The name of the table to be created in database
                          normally would be the current date, e.g 2020_05_03
               tabledate: The covid statistics, which should be like below >>
                          [[covid_data_of_country1],[covid_data_of_country2],
                           [covid_data_of_country3],...[covid_data_of_country_n]
                          ]
        No value will be returned
    """
    create_table = """CREATE TABLE IF NOT EXISTS %s(
                      id INT NOT NULL AUTO_INCREMENT,
                      country_region VARCHAR(100) NOT NULL,
                      total_cases INT NOT NULL DEFAULT '0' ,
                      active_cases INT NOT NULL DEFAULT '0',
                      total_death INT NOT NULL DEFAULT '0',
                      PRIMARY KEY(id));
                      """ % tablename
    cur.execute(create_table)
    insert = "INSERT INTO %s(country_region, total_cases,active_cases,total_death) "% tablename
    for each in tabledata:
        values = """VALUES ("%s", "%s", "%s", "%s");"""% tuple(each)
        sql = insert + values
        cur.execute(sql)
    cur.connection.commit()
    print('成功写入 %d 个国家疫情信息添加到表单【%s】' %(len(tabledata), tablename))

def updateMysql(port='localhost',user='root',pw=None,db='COVID_DATABASE'):
    """
    Acquire all covid statistics of all countries up to the current day
    make sure database has been created for storing data
    """
    today = '_'.join([str('%02d'%_) for _ in time.localtime()[:3]])
    url = 'https://www.worldometers.info/coronavirus/'
    web  = access_web(today, url)
    trs = get_target(web)
    urls = all_url(trs)
    all_countries = []
    for country, url in urls.items():
        country_web = access_web(country, url)
        info = find_data(country_web.text)
        single_country = (country, extract_data(info))
        save_data(country, single_country)
        print('【%s】疫情信息保存完毕'%country)
        all_countries.append(single_country)
        time.sleep(2)
    structured_covid_info = re_organizeData(all_countries)
    db = pymysql.connect(port, user, pw, db)
    cur = db.cursor()
    for day, details in structured_covid_info.items():
        dump_in(day, details)
    return structured_covid_info

def collect_data_into_one(maintable, targetable, exists=False):
    """BACKGROUND: if everyting goes right, where should be a number of tables
                   in the current database, which most of them are defined
                   according to date.
                   But there are 3 unique tables, which are 'maintables', all the
                   data from other tables will be collected into respective 'maintable'
                   by executing this function
           Note: This function is actually a sql(mysql query language) generator
       INSTRUCTIONS:
           parmas:
                 maintable: one of the 'maintables', e.g 'active_cases'.
                 targetable: the table contains covid statistics of a specific date
                             e.g '2020_05_03'.
                 exists: criteria for identifying if the maintable has existed or not
                         create one in not, and update it if exists
                         this param is default to Flase
           return: the existence status of the target maintable"""

    create_table = """CREATE TABLE %s(
                      country_region VARCHAR(100) NOT NULL DEFAULT "unknown",
                      PRIMARY KEY(country_region));
                      """ % maintable
    insert_countries = """INSERT INTO %s(country_region)
                          SELECT country_region FROM %s;
                          """ % (maintable, tables[-1])
    if not exists:
        try:
            cur.execute(create_table)
            print('创建表单 <%s> ' % maintable)
            cur.execute(insert_countries)
            exists = True
        except pymysql.err.InternalError as e:
            exists = True
    if_has_new_countries(maintable,targetable)
    alter = "ALTER TABLE %s ADD %s INT NOT NULL DEFAULT '0'; "% (maintable,targetable)
    try:
        cur.execute(alter)
    except pymysql.err.InternalError as e:
        print('%s 列已存在，尝试更新..'%targetable)
    update_sql = """UPDATE {0} LEFT JOIN {1}
                    ON {0}.country_region={1}.country_region
                    SET {0}.{1}=IFNULL({1}.{0}, 0);
                    """.format(maintable, targetable)
    temp = cur.execute(update_sql)
    cur.connection.commit()
    print('成功将 %d 个国家 %s 的疫情信息添加到表单【%s】' %(temp, targetable,maintable))
    return exists

def exclude_tables(tables):
    """purpose of this fuction:
            return a list of tables in current database which
            are defined by data
            e.g ['2020_01_22', '2020_01_23', ....,2020_05_03,..]
       param:
            tables:[[covid_data_of_country1],[covid_data_of_country2],
                    [covid_data_of_country3],...[covid_data_of_country_n]]
    """
    def formatables(tables):
        tables = [_[0] for _ in tables]
        return tables
    tables = formatables(tables)
    pattern = re.compile('\d{2}_\d{2}_\d{2}')
    final = []
    for table in tables:
        try:
            verify = pattern.search(table).group()
            final.append(table)
        except AttributeError as e:
            # print('remove <%s> from tables'% table)
            pass
    return final

def write_in_batch(target, raw_tables):
    exists = False
    for table in raw_tables:
        exists = collect_data_into_one(target, table, exists)

def organize_all_tables_into_one(port='localhost',user='root',pw=None,db='COVID_DATABASE'):
    """connect to database and generate sql to collect
       statistics of <total_cases>,<active_cases> and <total_death>
       from each table of respective date,
       then produce 3 seperate tables to store the collected statistics
       ---TABLE <total_cases>: all total cases from different date
       ---TABLE <active_cases>: all active cases from different date
       ---TABLE <total_death>: all total death data from different date"""
    db = pymysql.connect(port, user, pw, db)
    cur = db.cursor()
    cur.execute('show tables;')
    tables = cur.fetchall()
    tables = exclude_tables(tables)
    write_in_batch('total_cases', tables)
    write_in_batch('active_cases', tables)
    write_in_batch('total_death', tables)
    db.close()
    return tables

def if_has_new_countries(target_table,latest_table):
    """
        BACKGROUND: there are 233 countries and regions worldwid, however
                   covid will take time to spread to some areas.
                   e.g the first case in the world is 2020-01-22 in China,
                       the first csse in America is 2020-02-15.
                   this function will check if the covid statistics of the
                   latest date contain some countries that didn't listed in
                   maintable yesterday. If there are un-listed countries, add
                   them/it into maintable.
           Note: This function is actually a sql(mysql query language) generator
       INSTRUCTIONS:
           parmas:
                 target_table: one of the maintables, e.g 'active_cases'.
                 latest_table: normally would be the table of current date
                               e.g 2020_05_03, but it can be other data as well.
           return: None
    """

    sql = """INSERT INTO {target_table}(country_region)
             SELECT latest
                FROM
                    (SELECT
                        t1.country_region target,
                        t2.country_region latest
                    FROM
                        {target_table} t1
                        RIGHT JOIN {latest_table} t2
                    ON
                        t1.country_region=t2.country_region
                    WHERE
                        t1.country_region IS NULL)
             AS ref;""".format(target_table=target_table, latest_table=latest_table)
    res = cur.execute(sql)
    if res:
        print('%d newly added countries for table <%s>.' % (res, target_table))

def _change_name(tablename, column1='total_death', column2='total_death'):
    """定义这个函数纯粹只是为了修改之前的一些错误，death后面加了一个s
       但是death是不可数啊"""
    sql = """ALTER TABLE
                {tablename}
            CHANGE
                {column1}
                {column2} INT NOT NULL DEFAULT '0';
          """.format(tablename=tablename,column1=column1,column2=column2)
    cur.execute(sql)

def acquire_covid_data(): # main function to aquire latest covid statistics
    # db = pymysql.connect(port, user, pw, db)
    # cur = db.cursor()
    url = 'https://www.worldometers.info/coronavirus/'
    today = '_'.join([str('%02d'%_) for _ in time.localtime()[:3]])
    web = access_web(today, url)
    raw_data = get_target(web.text)
    COVID_data = all_data(raw_data)
    save_data(today,COVID_data)
    info_needed = [[_[0],_[1],_[6],_[3]] for _ in COVID_data]
    exists = True
    targetables = ['total_cases','active_cases','total_death']
    try:
        dump_in(today, info_needed)
        cur.execute('show tables;')
        tables = exclude_tables(cur.fetchall())
        # execute1 = [if_has_new_countries(_,tables[-1]) for _ in targetables]
        execute = [collect_data_into_one(_, today, exists) for _ in targetables]
    except TypeError as e:
        print('原始数据错误...',e)
    db.close()
    return COVID_data

if __name__ == "__main__":
    db = pymysql.connect('localhost','root','tyc1234','COVID_DATABASE')
    cur = db.cursor()
    # data = acquire_covid_data()
    url = 'https://www.worldometers.info/coronavirus/'
    today = '_'.join([str('%02d'%_) for _ in time.localtime()[:3]])
    #today = '2020_06_17'
    web = access_web(today, url)
    raw_data = get_target(web.text)
    COVID_data = all_data(raw_data)
    save_data(today,COVID_data)
    # info_needed = [[_[0],_[1],_[6],_[3]] for _ in COVID_data] #数据结构已更改
    info_needed = [[_[1],_[2],_[7],_[4]] for _ in COVID_data]
    exists = True
    targetables = ['total_cases','active_cases','total_death']
    try:
        dump_in(today, info_needed)
        cur.execute('show tables;')
        tables = exclude_tables(cur.fetchall())
        # execute1 = [if_has_new_countries(_,tables[-1]) for _ in targetables]
        execute = [collect_data_into_one(_, today, exists) for _ in targetables]
    except TypeError as e:
        print('原始数据错误...',e)
