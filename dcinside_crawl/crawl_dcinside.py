from bs4 import BeautifulSoup
import requests, grequests,re,sqlite3,datetime,sys,getopt
from collections import deque
headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Content-Type': 'text/html',
        'Host': 'gall.dcinside.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'}


conn = sqlite3.connect("../dbs/dcinside.db")
c = conn.cursor()

def getGameTitle(x):
    return re.sub("[^가-힣|^a-z|^A-Z|^0-9]","",x.text.strip())

def getGameBoardId_Url(x):
    myUrl = x["href"]
    myId = myUrl.split("=")[-1].strip()
    return myId,myUrl

def getSoups(x):
    try:
        html = x.text
        soup = BeautifulSoup(html,"html.parser")
        return soup
    except:
        return None

def dbAppend(r,i,gid):
    print("trying... %s data"%i,end="   ")
    try:
        html = r.text
        soup = BeautifulSoup(html,"html.parser")
        msg = soup.find("dd").text.strip() + soup.find("div",{"class":"con_substance"}).text.strip()
        dt = soup.find("div",{"class":"w_top_right"}).text.strip()[:19]
    
    except:
        print("fail")
        return 0
    gameId = gid
    datas = (gameId,msg,dt,i)
    print("success")
    return datas





def getPageSoup(myUrl):
    headers_ = {
    'Content-Type': 'text/html',
        
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'}
    req = requests.get(myUrl,headers=headers_)
    html = req.text
    soup = BeautifulSoup(html,"html.parser")
    return soup

def getGameBoardUrls():
    gameBoardUrls = []
    soup = getPageSoup("http://wstatic.dcinside.com/gallery/gallindex_iframe_new_gallery_game.html")
    idx = 1
    while True:
        boardList = soup.find("td",{"class":"game"+str(idx)})
        if boardList == None:
            break
        boardListTitle =  boardList.find_all("a",{"class":"list_title"})
        urls_i = list(map(lambda x: (*getGameBoardId_Url(x),getGameTitle(x)) ,boardListTitle))
        gameBoardUrls += urls_i
        idx+=1
    

    soup = getPageSoup("http://wstatic.dcinside.com/gallery/mgallindex_iframe_game.html")
    boardListTitle = soup.find_all("a",class_="list_title")
    urls_i = list(map(lambda x: (*getGameBoardId_Url(x),getGameTitle(x)) ,boardListTitle))
    gameBoardUrls += urls_i
    return gameBoardUrls


def insertGameMsgs(lastestBoardNum,gid):
    global conn
    global c
    myUrl = "http://gall.dcinside.com/board/view/?id=%s&no=%d"
    print("starting...")
    lastMsgNoLmbda = lambda x : 1 if x==None else x+1
    idx = lastMsgNoLmbda(c.execute("select Max(MsgNo) from UserMsg").fetchone()[0])
    batchSize = 100
    isEnd = False
    maxNum = lastestBoardNum
    while True:
        rs = (grequests.get(myUrl%(gid,i),headers=headers,timeout=2) for i in range(idx,idx+batchSize))
        myresponse = grequests.map(rs)
        datas = []
        for res,i in zip(myresponse,range(idx,idx+batchSize)):
            data_i = dbAppend(res,i,gid)
            if data_i != 0:
                datas.append(data_i)
        c.executemany("INSERT INTO UserMsg VALUES(?,?,?,?)",datas)
        c.execute("select count(*) from UserMsg")
        print(c.fetchone()[0])
        print(datetime.datetime.now())
        idx += batchSize
        conn.commit()
        if isEnd:
            break
        if idx >= maxNum:
            isEnd = True


def whichUrl(arg):
    global conn
    global c
    try:
        gameboardUrl = [i for i in c.execute('SELECT url FROM GameProfile where id="%s" OR title LIKE "%%%s%%"'%(arg,arg))][0][0]
        return gameboardUrl
    except:
        print("invalid game-id or game title name")
        return "0"

def getLastestBoardNum(boardUrl):
    req = requests.get(boardUrl,headers=headers)
    html = req.text
    soup = BeautifulSoup(html,"html.parser")
    boardnums = list(map(lambda x: "".join(re.findall("[0-9]",x.text)) ,soup.find_all("td",{"class":"t_notice"})))
    maxnum = -1
    for i in boardnums:
        try:
            maxnum = max(int(i),maxnum)
        except:
            pass
    return maxnum

def main():
    global conn
    global c
    try:
        opts, args = getopt.getopt(sys.argv[1:],"ut:",["title="])
    except getopt.GetoptError as err:
        print(str(err))
        sys.exit(1)

    opt,arg = opts[0]
    for opt, arg in opts:
        if opt == "-u":
            c.executemany("INSERT INTO GameProfile VALUES(?,?,?)",getGameBoardUrls())
            conn.commit()
        
        if opt == "-t":
            boardUrl = whichUrl(arg);
            if boardUrl == "0":
                sys.exit(1)
            lastestBoardNum = getLastestBoardNum(boardUrl)
            insertGameMsgs(lastestBoardNum,arg)
    conn.close()
    return ;







if __name__ == "__main__":
    main()





