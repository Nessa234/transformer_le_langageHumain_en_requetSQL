import pandas as pd
import numpy as np
import re
import sys
import json
from sklearn . model_selection import train_test_split ,GridSearchCV
from sklearn . feature_extraction . text import CountVectorizer , TfidfVectorizer
from sklearn . linear_model import Perceptron
from sklearn . pipeline import Pipeline
import os
from os import pipe
import duckdb

def convert(requet, listes):

    """prend en entree une requˆete en langage naturel et qui produit 2 resultats : la
    meme requete ou toutes les valeurs de la base de donn´ees sont remplacees par leur type ;
    la liste des concepts/valeurs contenus dans cette phrase"""

    concep_value = []

    for i in listes.keys():
        for j in listes[i]:
            if re.search(rf"\b{j}\b", requet):
                val=""
                if i=='année':
                    val='year'
                elif i== 'genre':
                    val= 'type'
                elif i=='titre':
                    val= 'name'
                elif i== 'réalisateur':
                    val= 'producteur'
                else :
                    val= "player"
                concep_value.append({'value': j, 'concept': val})
                requet = requet.replace(str(j), val)
    return concep_value, requet

def find_label(filename,listes):
    """la liste de labels differents pour les intentions select et where"""

    with open(filename) as f:
        data = json.load(f)

    selecte, were,X = [], [],[]

    for entry in data:
        X.append(entry['french']['query_french'])
        commande = entry['sql']
        a=[] #va recevoir les valeur qui sont compirs entre SELECT et FROM
        select_part = commande.split("FROM")[0].replace("SELECT", "").strip()
        a.extend(select_part.split(","))
        res=""
        for mot in a:
            res+=mot+" "
        selecte.append(res)
        match = re.search(r'\bWHERE\b\s*(.*)', commande, re.IGNORECASE)
        if match:
            matche= match.group(1)

            were.append(convert(matche, listes)[1])
        for phrase in entry['french']['paraphrase_french']:
            X.append(phrase)
            selecte.append(res)
            were.append(convert(matche, listes)[1])

    return selecte, were, X


def model(selecte, where, X):
    X_train_sel,X_test_sel,y_train_sel,y_test_sel= train_test_split(X,selecte,test_size =0.3 , random_state =42)
    X_train_whe,X_test_whe,y_train_whe,y_test_whe= train_test_split(X,where,test_size =0.3 , random_state =42)
    train_selecte= Pipeline ([( " vectorizer " , TfidfVectorizer () ) ,( " perceptron " , Perceptron () )])
    train_where = Pipeline ([( " vectorizer " , TfidfVectorizer () ) ,( " perceptron " , Perceptron () )])
    
    train_selecte.fit(X_train_sel,y_train_sel)
    train_where.fit(X_train_whe,y_train_whe)
    return train_selecte ,train_where



def create_table(df):
    acteurs=np.concatenate((df['acteur1'].values,df['acteur2'].values))
    acteurs= np.concatenate((acteurs,df['acteur3'].values))
    listes= {'titre': df['titre'].values, 'année': df['annee'].values, 'genre': df['genre'].values, 'réalisateur': df['realisateur'].values, 'acteur': acteurs}

    return listes


def produire_requet(train_selecte,train_where,li, phrase):
    sel_pred= train_selecte.predict([phrase])[0].split()
    select= sel_pred[0]
    if len(sel_pred)>1:
        for j in range(1,len(sel_pred)):
            select+=" , "
            select+= sel_pred[j]
    where_pred= train_where.predict([phrase])[0]
    concept_value= convert(phrase,li)[0]
    mot= 'AND'
    motif = r'\b' + re.escape(mot) + r'\b'
    nb_mot= len(re.findall(motif, where_pred, flags=re.IGNORECASE))
    res=""
    if len(concept_value)==0:
        pass
    elif nb_mot==1 and len(concept_value)==2:
        if concept_value[0]['concept']!=concept_value[1]['concept']:
            for i in concept_value:
                where_pred=where_pred.replace(i['concept'],str(i['value']))
        else :
            avant,apres= where_pred.split("AND")
            avant=avant.replace(concept_value[0]['concept'],str(concept_value[0]['value']))
            apres=apres.replace(concept_value[1]['concept'],str(concept_value[1]['value']))
            where_pred= avant+" AND "+apres
    else:
        valu=str(concept_value[0]['value'])
        where_pred=where_pred.replace(concept_value[0]['concept'],valu)
    res= 'SELECT '+ select+ ' FROM films '+' WHERE '+where_pred+' '
    return res

def interogation_dataBase(filename,phrase,train_selecte,train_where,li):
    films = pd.read_csv(filename)
    sql_requet= produire_requet(train_selecte,train_where,li, phrase)
    result = duckdb.query(sql_requet).to_df()
    return result
    
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("❗ Utilisation : python script.py base_films_500.csv 'Votre question ici'")
        sys.exit(1)
    boul= "Y"
    chemin_csv =sys.argv[1]
    questions = ' '.join(sys.argv[2:])
    df=df= pd.read_csv( chemin_csv)   
    li=create_table (df)
    selecte, were, X= find_label("queries_french_para.json",li)
    train_selecte ,train_where= model(selecte, were, X)
    question=questions
    while boul=="Y":
        a=interogation_dataBase(chemin_csv,question,train_selecte,train_where,li)
        print(a)
        x=input("Voulez-vous continuer? O/N")
        if x=="y" or x=="Y" or x== "O" or x=="o":
            boul="Y"
            question= input("que voulez vous trouver ? ")
        else :
            boul=x 
    
