import nltk
import pymysql


def calculaPageRank(iteracoes):
    con = pymysql.connect(host='localhost', user='root',
                          passwd='root', db='indice', autocommit=True)

    cursor = con.cursor()
    cursor.execute("delete from page_rank")
    cursor.execute("insert into page_rank select id_url, 1.0 from urls")

    for i in range(iteracoes):
        cursorUrl = con.cursor()
        cursorUrl.execute("select id_url from urls")
        for url in cursorUrl:
            pageRank = 0.15

            cursorLinks = con.cursor()
            cursorLinks.execute("select distinct(id_url_origem) from "
                                "url_ligacao where id_url_destino = %s",
                                url[0])

            for link in cursorLinks:
                cursorPageRank = con.cursor()
                cursorPageRank.execute("select nota from page_rank where id_url = %s", link[0])

                linkPageRank = cursorPageRank.fetchone()[0]

                cursorQuantidade = con.cursor()
                cursorQuantidade.execute("select count(*) from url_ligacao where id_url_origem=%s", link[0])

                linkQuantidade = cursorQuantidade.fetchone()[0]
                pageRank += 0.85 * (linkPageRank / linkQuantidade)

            cursorAtualizar = con.cursor()
            cursorAtualizar.execute("update page_rank set nota =%s where id_url =%s", (pageRank, url[0]))

    cursorAtualizar.close()
    cursorQuantidade.close()
    cursorPageRank.close()
    cursorUrl.close()
    cursorLinks.close()
    cursor.close()
    con.close()


def getUrl(id_url):
    retorno = ''
    con = pymysql.connect(host='localhost', user='root',
                          passwd='root', db='indice', autocommit=True)
    cursor = con.cursor()
    cursor.execute("select url from urls where id_url = %s", id_url)

    if cursor.rowcount > 0:
        retoro = cursor.fetchone()[0]

    cursor.close()
    con.close()

    return retorno


def normalizarMaior(notas):
    menor = 0.00001
    maximo = max(notas.values())
    if maximo == 0:
        maximo = menor
    return dict([(id, float(nota) / maximo) for (id, nota) in notas.items()])


def normalizarMenor(notas):
    menor = 0.00001
    minimo = min(notas.values())
    return dict([(id, float(minimo) / max(menor, nota)) for (id, nota) in notas.items()])


def frequenciaScore(linhas):
    contagem = dict([(linha[0], 0) for linha in linhas])
    for linha in linhas:
        contagem[linha[0]] += 1
    return normalizarMaior(contagem)


def localizacaoScore(linhas):
    localizacoes = dict([linha[0], 1000000] for linha in linhas)

    for linha in linhas:
        soma = sum(linha[1:])
        if soma < localizacoes[linha[0]]:
            localizacoes[linha[0]] = soma
    return normalizarMenor(localizacoes)


def distanciaScore(linhas):
    contagem = dict([linha[0], 1.0] for linha in linhas)

    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice')
    cursor = conexao.cursor()

    for i in contagem:
        cursor.execute("select count(*) from url_ligacao where id_url_destino = %s", i)
        if __name__ == '__main__':
            contagem[i] = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return normalizarMenor(contagem)


def contagemLinkScore(linhas):
    contagem = dict([linha[0], 1.0] for linha in linhas)
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice')
    cursor = conexao.cursor()

    for i in contagem:
        cursor.execute("select count(*) from url_ligacao where id_url_destino = %s", i)
        contagem[i] = cursor.fetchone()[0]

    cursor.close()
    conexao.close()
    return normalizarMaior(contagem)


def pageRankScore(linhas):
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice')
    cursor = conexao.cursor()

    pageRanks = dict([linha[0], 1.0] for linha in linhas)

    for i in pageRanks:
        cursor.execute("select nota from page_rank where id_url = %s", i)
        pageRanks[i] = cursor.fetchone()[0]

    cursor.close()
    conexao.close()
    return normalizarMaior(pageRanks)


def textoLinkScore(linhas, palavras_id):
    contagem = dict([linha[0], 0] for linha in linhas)
    con = pymysql.connect(host='localhost', user='root',
                          passwd='root', db='indice', autocommit=True)

    for id in palavras_id:
        cursor = con.cursor()

        cursor.execute("select ul.id_url_origem, ul.id_url_destino "
                       "from url_palavra up "
                       "inner join url_ligacao ul "
                       "on ul.id_url_ligacao = up.id_url_ligacao "
                       "and up.id_palavra = %s", id)

        for (id_url_origem, id_url_destino) in cursor:

            if id_url_destino in contagem:
                cursorRank = con.cursor()
                cursorRank.execute("select nota from page_rank where id_url = %s", id_url_origem)
                pageRank = cursorRank.fetchone()[0]
                contagem[id_url_destino] += pageRank

    cursorRank.close()
    cursor.close()
    con.close()
    return normalizarMaior(contagem)


def pesquisa(consulta):
    linhas, palavras_id = buscaMaisPalavras(consulta)

    scores = textoLinkScore(linhas, palavras_id)

    score_ordenado = sorted([(score, url) for (url, score) in scores.items()], reverse=1)

    for (score, url) in score_ordenado[0:10]:
        print('%f\t%s' % (score, getUrl(url)))


def pesquisaPeso(consulta):
    linhas, palavras_id = buscaMaisPalavras(consulta)
    total_score = dict([linha[0], 0] for linha in linhas)

    pesos = [
        (1.0, frequenciaScore(linhas)),
        (1.0, localizacaoScore(linhas)),
        (1.0, distanciaScore(linhas)),
        (1.0, contagemLinkScore(linhas)),
        (5.0, pageRankScore(linhas)),
        (1.0, textoLinkScore(linhas, palavras_id)), ]

    for (peso, score) in pesos:
        for url in total_score:
            total_score[url] += peso * score[url]

    score_ordenado = sorted([(score, url) for (url, score) in total_score.items()], reverse=1)

    for (score, url) in score_ordenado[0:10]:
        print('%f\t%s' % (score, getUrl(url)))


def buscaMaisPalavras(consulta):
    pass


def getIdPalavra(palavra):
    retorno = -1
    stemmer = nltk.stem.RSLPStemmer()
    con = pymysql.connect(host='localhost', user='root',
                          passwd='root', db='indice', autocommit=True)
    cursor = con.cursor()
    cursor.execute("select id_palavra from palavras where palavra = %s",
                   stemmer.stem(palavra))

    if cursor.rowcount > 0:
        retorno = cursor.fetchone()[0]

    cursor.close()
    con.close()

    return retorno


def buscaSimples(palavra):
    id_palavra = getIdPalavra(palavra)
    con = pymysql.connect(host='localhost', user='root',
                          passwd='root', db='indice', autocommit=True)
    cursor = con.cursor()
    cursor.execute('select urls.url from palavra_localizacao plc '
                   'inner join urls on plc.id_url = urls.id_url '
                   'where plc.id_palavra = %s', id_palavra)

    paginas = set()

    for url in cursor:
        paginas.add(url[0])

    print(f"Páginas encontradas: {len(paginas)}")
    for url in paginas:
        print(url)

    cursor.close()
    con.close()
