# covid19-status
This reposotory is meant to collect covid statistics from 'worldmeter' and further analyze the trending of the pandemic situation\n

Right now only finished with collecting of data from worldmeter\n

some notes on this module:\n
    1. Installation of mysql is required, and a database named 'covid_update' has to be set up in advance as well.\n
    2. There are three main functions, which are "__all__ = ['acquire_covid_data','updateMysql','organize_all_tables_into_one']"\n
       the other functions can also work but it might be confusing. well it is confusing to me anyway.\n
    3. Use of funtion 'updateMysql' will generate a lot of temp files, it is suggested to change into a temp foler prior to commencement of this funtion\n
    3. There are some useless functions and codes still reserved in the file, they are useless, but I think I might as well keep it. It witnessed my growth.\n
