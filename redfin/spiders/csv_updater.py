import scrapy
from csv import reader
import json
from urllib.parse import urljoin
from urllib.request import urlopen

class CsvUpdaterSpider(scrapy.Spider):
    name = 'csv_updater'
    allowed_domains = ['redfin.com', 'google.com']
    start_urls = ['https://www.redfin.com']

    def parse(self, response, **kwargs):
        i = 0
        url = 'https://raw.githubusercontent.com/ubaiidullaah/redfin/master/redfin/Property%20Status%20Report.csv'
        response = urlopen(url)
        lines = [l.decode('utf-8') for l in response.readlines()]
        csv_reader = reader(lines)
        for row in csv_reader:
            if i == 0:
                i += 1
                continue
            d = {
                'Record Id': row[0],
                'Created Time': row[1],
                'HSLP ordered at': row[2],
                'Platform database ID (Agent)': row[3],
                'Display Name': row[4],
                'Property City': row[5],
                'State (Agent)': row[6],
                'Closing Date': row[7],
                'Property Status': row[8],
                'Property Status Updated': row[9],
                'Closing Date is Wrong': row[10]
            }
            # print(d)
            house = row[4] + ' ' + row[5]
            url = f'https://www.redfin.com/stingray/do/location-autocomplete?location={house}&start=0&count=10&v=2&market=houston&al=1&iss=false&ooa=true&mrs=false&lat=29.281849&lng=-94.803532'
            yield scrapy.Request(url=url, callback=self.requester, meta=d)
            

    def requester(self, response):
        text = response.text[4:]
        pd = json.loads(text)
        try:
            try:
                rurl = pd['payload']['exactMatch']['url']
            except:
                rurl = pd["payload"]["sections"][0]["rows"][0]["url"]
            url = urljoin('https://redfin.com', rurl)
        except:
            url = 'https://google.com'
        d = {
            'Record Id': response.meta['Record Id'],
            'Created Time': response.meta['Created Time'],
            'HSLP ordered at': response.meta['HSLP ordered at'],
            'Platform database ID (Agent)': response.meta['Platform database ID (Agent)'],
            'Display Name': response.meta['Display Name'],
            'Property City': response.meta['Property City'],
            'State (Agent)': response.meta['State (Agent)'],
            'Closing Date': response.meta['Closing Date'],
            'Property Status': response.meta['Property Status'],
            'Property Status Updated': response.meta['Property Status Updated'],
            'Closing Date is Wrong': response.meta['Closing Date is Wrong']
        }
        # print(d)
        yield scrapy.Request(url=url, callback=self.yielder, meta=d)

    def yielder(self, response):
        property_status = response.meta['Property Status']
        date_updated = response.meta['Property Status Updated']

        try:
            try:
                property_status = response.xpath('//div[@class="keyDetailsList"][1]/div[1]/span[2]/div/span/text()').get().strip()
            except:
                property_status = response.xpath('//span[contains(text(), "Status")]/following-sibling::span/text()').get().strip()
        except:
            pass

        try:
            try:
                d_list = response.xpath('//div[@class="data-quality"]/text()').getall()[-1].split()[-5:-2]
                date_updated = f'{d_list[1].replace(",","")}-{d_list[0]}-{d_list[2]}'.strip().replace('(','')
            except:
                d_list = response.xpath('//div[@class="source-info"]/text()').getall()[-2].split()
                date_updated = f'{d_list[1].replace(",","")}-{d_list[0]}-{d_list[2]}'.strip().replace('(','')
        except:
            pass
        yield {
            'Record Id': response.meta['Record Id'],
            'Created Time': response.meta['Created Time'],
            'HSLP ordered at': response.meta['HSLP ordered at'],
            'Platform database ID (Agent)': response.meta['Platform database ID (Agent)'],
            'Display Name': response.meta['Display Name'],
            'Property City': response.meta['Property City'],
            'State (Agent)': response.meta['State (Agent)'],
            'Closing Date': response.meta['Closing Date'],
            'Property Status': property_status,
            'Property Status Updated': date_updated,
            'Closing Date is Wrong': response.meta['Closing Date is Wrong']
        }
