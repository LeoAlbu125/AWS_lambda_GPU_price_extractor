import json
import pandas as pd
import requests as rq
import datetime as dt
import zoneinfo as zi
from bs4 import BeautifulSoup as bs
import cloudscraper as cs
import boto3

#/class="olx-text olx-text--body-large olx-text--block olx-text--semibold olx-ad-card__price"
#/class="olx-text olx-text--caption olx-text--block olx-text--regular olx-ad-card__old-price olx-ad-card__old-price--vertical"
#/class="olx-text olx-text--body-large olx-text--block olx-text--semibold olx-ad-card__price"
#/class="olx-text olx-text--body-large olx-text--block olx-text--semibold olx-ad-card__price"


def lambda_handler(event, context):

    date_today_BRSP = dt.datetime.now(zi.ZoneInfo('America/Sao_Paulo'))
    date_today_BRSP_normal = dt.date.now(zi.ZoneInfo('America/Sao_Paulo'))
    print("today`s date: {}".format(date_today_BRSP))
    pages = event['pages_qtd']
    extracted_df = extractor(pages)
    extracted_df.to_csv('/tmp/results_{}.csv'.format(date_today_BRSP_normal))
    s3 = boto3.resource('s3')
    s3.Bucket("tempiwilldeletesoon").upload_file('/tmp/results_{}.csv'.format(date_today_BRSP_normal),'results_{}.csv'.format(date_today_BRSP_normal))
    return extracted_df

def extractor(pages_num):
    pages = pages_num
    
    base_url = "https://www.olx.com.br/autos-e-pecas/motos"
    
    final_list = []
    for x in range(0,pages):
        res = cs.create_scraper()
        if x == 0:
            res = res.get(base_url)
        else:
            res = res.get(base_url+"?o={}".format(x+1))
        res_parsed = bs(res.content,"html.parser")
        names_list = []
        price_list = []
        
        for idx,href_el in enumerate(res_parsed.find_all("a",class_='olx-ad-card__link-wrapper')):
            names_list.append([href_el['href'].split('/')[-1],href_el['href']])
            
        for idx,el in enumerate(res_parsed.find_all("div",class_='olx-ad-card__details-price--vertical')):
            temp_list = []
    
            if el.find('p') == None:
                temp_list.append(None)
            else:
                temp_list.append(el.find('p').get_text())
                
            if el.find('h3') == None:
                temp_list.append(None)
            else:
                temp_list.append(el.find('h3').get_text()) 
                
            price_list.append(temp_list)

        for idx in range(0,len(names_list)):
            final_list.append(names_list[idx]+price_list[idx]+[date_today_BRSP])

    df = pd.DataFrame(final_list,columns=['name','url','old_price','new_price','extraction_date'])
    
    return df  # Echo back the first key value
