# wtml
Where's the money Lebowksi?
1) Run gift_scraper.py (produces travel_reports.csv)
2) run upload.py (moves travel_reports.csv to SUPABASE)
3) run member_links_scraper.py (produces member_links.csv)
4) run member_details.py (uses member_links to find links to scrape for member details and then moves them to SUPABASE)


NEW ORDER 4/25/25 - I realized as i expanded my thought as to what I want this site to be, i was doing the scraping backwards
On 4/25 I re-ordered my thinking.
1) Run member_links_scraper.py to create member_links.csv (a list of all member bio pages)
2) Run member_details.py which uses member_links.csv as a guide to the bio links to scrape - I've also modified
this file so that it is generating a unique ID for each member scraped (this is necessary because in theory i'm 
scraping the bios of all the members of congress not just those who filed a gift expense report which wouldn't be
everyone so it made no sense to create a unique id from that data)
3) Run gift_scraper.py (produces travel-reports.csv)
4) Run upload.py (moves travel_reports to SUPABASE: house_travel_reports and moves some of the records to member_staff table)
