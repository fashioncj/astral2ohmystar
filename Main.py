# ！coding:utf-8
import sqlite3
import json
import sys, getopt
import os


def create_db(jsonfile, username):
    myjson = json.load(open(jsonfile))

    conn = sqlite3.connect(
        os.path.abspath(os.path.dirname(jsonfile) + os.path.sep + ".") + os.path.sep + username + ".db")
    cursor = conn.cursor()

    # create table
    cursor.execute("CREATE TABLE omstag(tagName string)")
    cursor.execute("CREATE TABLE omsrepo(id integer32 PRIMARY KEY,repoTags string,belongGroupIDs string)")
    cursor.execute("CREATE TABLE omsgroup(itemID INTEGER PRIMARY KEY,groupName string,groupSequence integer32)")
    cursor.execute(
        "CREATE TABLE dbInfo(fileVersion string,login string,groupCount INTEGER,repoCount INTEGER,tagCount INTEGER,createTime timestemp NOT NULL DEFAULT (datetime('now','localtime')))")
    cursor.execute('INSERT INTO omsgroup (itemID,groupName,groupSequence) VALUES (0,"Untag",0)')
    cursor.execute('INSERT INTO omstag (tagName) VALUES ("Untag")')
    tagSum = 1
    repoSum = 0

    for item in myjson:
        tags = myjson[item]["tags"]
        tagID = ""
        tagName = "";

        for tag in tags:
            cursor.execute('SELECT * FROM omsgroup WHERE groupName=?', (tag["name"],))
            values = cursor.fetchall()
            if len(values) < 1:
                cursor.execute('INSERT INTO omsgroup (groupName,groupSequence) VALUES (?,?)',
                               (tag["name"], tag["sort_order"]))
                cursor.execute('INSERT INTO omstag (tagName) VALUES (?)', (tag["name"],))
                tagSum += 1

            cursor.execute('SELECT itemID FROM omsgroup WHERE groupName=?', (tag["name"],))
            tagID = tagID + "," + str(cursor.fetchone()[0]);
            tagName += "," + tag["name"]

        if len(tagID) < 1:
            tagID = "0"
            tagName = "Untag"
        else:
            tagID = tagID[1:]
            tagName = tagName[1:]

        repoSum += 1
        cursor.execute('INSERT INTO omsrepo (id,repoTags,belongGroupIDs) VALUES (?,?,?)',
                       (int(myjson[item]["repo_id"]), tagName, tagID))

    cursor.execute('INSERT INTO dbinfo (fileVersion,login,groupCount,repoCount,tagCount) VALUES (?,?,?,?,?)',
                   ("1.0.0", username, tagSum, repoSum, tagSum))

    cursor.close()
    conn.commit()
    conn.close()

    os.rename(os.path.abspath(os.path.dirname(jsonfile) + os.path.sep + ".") + os.path.sep + username + ".db",
              os.path.abspath(os.path.dirname(jsonfile) + os.path.sep + ".") + os.path.sep + username + ".omsbackup")
    print "DB locate at@" + os.path.abspath(
        os.path.dirname(jsonfile) + os.path.sep + ".") + os.path.sep + username + ".omsbackup"


if __name__ == '__main__':
    opts, args = getopt.getopt(sys.argv[1:], "hi:u:")

    input_file = "";
    username = ""

    for op, value in opts:
        if op == "-i":
            input_file = value
        elif op == "-u":
            username = value

    if len(input_file) > 0 and len(username) > 0:
        create_db(input_file, username)
    else:
        print "Wrong args!"
        sys.exit()
