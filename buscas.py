import pymysql


def calculaPageRank(iteracoes):
    con = pymysql.connect(host='localhost', user='root',
                          passwd='root', db='indice', autocommit=True)

    cursos = con.cursor()
