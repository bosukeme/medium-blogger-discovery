import twint
import nest_asyncio
import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
from urllib.parse import urlparse
from newspaper import Article
import time, os
from pymongo import MongoClient
from dotenv import load_dotenv
nest_asyncio.apply()


load_dotenv(".env", verbose=True)
MONGO_URL = os.environ['Mongo_URL']
client = MongoClient(MONGO_URL, retryWrites=False, connect=False)

db = client.medium_blogger_discovery


today = datetime.now() 
yesterday = datetime.now() - timedelta(1)
last_week = datetime.now() - timedelta(7)
today_date = datetime.strftime(today, '%Y-%m-%d')
yesterday_date = datetime.strftime(yesterday, '%Y-%m-%d')
last_week_date = datetime.strftime(last_week, '%Y-%m-%d')

num_tweets = 5000 
num_posts = 100 


medium_search_list = ['medium post', 'medium article', 'medium blog', 'medium.com']
blog_search_list = ['blog post', 'article', 'blog']
substack_search_list = ['substack']

search_type_list = ['Medium', 'Substack', 'Blog']
search_terms_list = [medium_search_list, substack_search_list, blog_search_list]

content_type_search_dict = dict(zip(search_type_list, search_terms_list))


def available_columns():
    return twint.output.panda.Tweets_df.columns


def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]


def get_followings(username):

    c = twint.Config()
    c.Username = username
    c.Pandas = True

    twint.run.Following(c)
    list_of_followings = twint.storage.panda.Follow_df

    return list_of_followings['following'][username]


def get_latest_tweets_from_handle(username, num_tweets, date):

    c = twint.Config()
    c.Username = username
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = date
    c.Hide_output = True

    twint.run.Search(c)
    
    try:
        tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags', 
               'username', 'name', 'link', 'urls', 'photos', 'video',
               'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])
    except:
        tweet_df = pd.DataFrame()
        
    return tweet_df


def create_search_strings_from_tweet_df(tweet_df):
    search_string_list = []
    for i in range(len(tweet_df)):
        tweet_text = tweet_df.iloc[i]['tweet']
        search_string = " ".join(tweet_text.split()[0:5])
        search_string_list.append(search_string)

    tweet_df['Search String'] = search_string_list
    return tweet_df


def get_tweets_from_search_term(search_term, num_tweets, date):
    """
    This function does a search on twitter and returns the handles of those who
    posted the tweets that matched the search term the top 5 most liked tweets for
    each search term
    
    ** Come back to this later and remove the part that only picks 
    """
    c = twint.Config()
    c.Search = search_term
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = date
    c.Hide_output = True

    twint.run.Search(c)
    
    try:
        search_tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags', 
               'username', 'name', 'link', 'urls', 'photos', 'video',
               'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])

        tweet_w_url_inds = []
        for i in range(len(search_tweet_df)):
            url = search_tweet_df.iloc[i]['urls']
            if len(url) > 0:
                tweet_w_url_inds.append(i)

        search_tweet_df_final = search_tweet_df.iloc[tweet_w_url_inds]
        search_tweet_df_final = search_tweet_df.sort_values(by=['nlikes'], ascending=True) # sort by likes in ascending
    except:
        search_tweet_df_final = []
    
    return search_tweet_df_final


def get_tweets_for_date(tweet_df, date):
    """
    This function takes a date and returns all the tweets from that date
    """
    date_tweet_inds = []
    for i in range(len(tweet_df)):
        tweet_date = tweet_df['date'].iloc[i][0:10]
        if tweet_date == date:
            date_tweet_inds.append(i)
    
    date_tweet_df = tweet_df.iloc[date_tweet_inds]
    
    return date_tweet_df


def cleanup_medium_tweets(tweet_df, num_posts):
    """
    This gets the potential tweets from medium users and then 
    filters out the ones that dont have a medium link in them
    """
    medium_tweet_inds = []
    for i in range(len(tweet_df)):
        tweet_url_list = tweet_df['urls'].iloc[i]

        if len(tweet_url_list) > 0:
            for url in tweet_url_list:
                if 'medium.com' in url:
                    medium_tweet_inds.append(i)
                    
    medium_tweet_inds = list(set(medium_tweet_inds))
    medium_tweet_df = tweet_df.iloc[medium_tweet_inds]
    
    ## For now we first want to filter out and only work with tweets that are in english
    english_tweet_df = medium_tweet_df[medium_tweet_df['language']=='en']
    english_tweet_df = english_tweet_df.sort_values(by=['nlikes'], ascending=False)
    
    top_english_tweet_df = english_tweet_df.iloc[0:int(num_posts/2)]
    bottom_english_tweet_df = english_tweet_df.iloc[-int(num_posts/2):]
    medium_tweet_df = pd.concat([top_english_tweet_df, bottom_english_tweet_df])
    
    medium_tweet_df = medium_tweet_df.sort_values(by=['nlikes'], ascending=False)
    
    return medium_tweet_df


def cleanup_substack_tweets(tweet_df, num_posts):
    """
    This gets the potential tweets from medium users and then 
    filters out the ones that dont have a medium link in them
    """
    substack_tweet_inds = []
    for i in range(len(tweet_df)):
        tweet_url_list = tweet_df['urls'].iloc[i]

        if len(tweet_url_list) > 0:
            for url in tweet_url_list:
                if 'substack' in url:
                    substack_tweet_inds.append(i)
                    
    substack_tweet_inds = list(set(substack_tweet_inds))
    substack_tweet_df = tweet_df.iloc[substack_tweet_inds]
    
    ## For now we first want to filter out and only work with tweets that are in english
    english_tweet_df = substack_tweet_df[substack_tweet_df['language']=='en']
    english_tweet_df = english_tweet_df.sort_values(by=['nlikes'], ascending=False)
#     print('Number of english tweets -> %s' % len(english_tweet_df))
    
    top_english_tweet_df = english_tweet_df.iloc[0:int(num_posts/2)]
    bottom_english_tweet_df = english_tweet_df.iloc[-int(num_posts/2):]
    substack_tweet_df = pd.concat([top_english_tweet_df, bottom_english_tweet_df])
    
    # Sort them by number of likes
    substack_tweet_df = substack_tweet_df.sort_values(by=['nlikes'], ascending=False)
    
    return substack_tweet_df


def cleanup_blog_tweets(tweet_df, num_posts):
    """
    This gets the potential tweets from medium users and then 
    filters out the ones that dont have a medium link in them
    
    num_posts: this depicts how many posts we want to extract, we can scale this up as the capacity of the marketing channels get better
    """
    ## First process the tweet df and remove any tweets that dont have links in them
    link_tweet_inds = []
    for i in range(len(tweet_df)):
        tweet_url_list = tweet_df['urls'].iloc[i]
        
        if len(tweet_url_list) > 0:
            link_tweet_inds.append(i)
    
    link_tweet_inds = list(set(link_tweet_inds))
    link_tweet_df = tweet_df.iloc[link_tweet_inds]
#     print('Number of link tweets -> %s' % len(link_tweet_df))
    
    ## For now we first want to filter out and only work with tweets that are in english
    english_tweet_df = link_tweet_df[link_tweet_df['language']=='en']
    english_tweet_df = english_tweet_df.sort_values(by=['nlikes'], ascending=False)
#     print('Number of english tweets -> %s' % len(english_tweet_df))
    
    top_english_tweet_df = english_tweet_df.iloc[0:int(num_posts/2)]
    bottom_english_tweet_df = english_tweet_df.iloc[-int(num_posts/2):]
    tweet_df = pd.concat([top_english_tweet_df, bottom_english_tweet_df])
#     print('Number of processing tweets -> %s' % len(tweet_df))
    ## Now we get only the top 50 and bottom 50, this is all we process for now
    
    blog_tweet_inds = []
    for i in range(len(tweet_df)):

        tweet_url_list = tweet_df['urls'].iloc[i]
        
        # Process the link to check if it passes the parameters of what a blog post should be
        try:
            article = Article(tweet_url_list[0])
            article.download()
            article.parse()
            top_image = article.has_top_image()
            text_len = len(article.text)

            if top_image and (text_len > 1000):
                blog_tweet_inds.append(i)
                
        except Exception as e:
            pass


    blog_tweet_inds = list(set(blog_tweet_inds))
    blog_tweet_df = tweet_df.iloc[blog_tweet_inds]

    # Sort them by number of likes
    blog_tweet_df = blog_tweet_df.sort_values(by=['nlikes'], ascending=False)
    
    return blog_tweet_df


def process_tweets_from_content(content_type, search_list, num_tweets, num_posts, yesterday_date):
    
    tweet_df_list = []
    for search_term in search_list:
        tweet_df = get_tweets_from_search_term(search_term, num_tweets, yesterday_date)
        tweet_df_list.append(tweet_df)

    search_tweet_df = pd.concat(tweet_df_list)

    # Drop duplicates from the output
    search_tweet_df.drop_duplicates(subset=['id'], keep=False)

    ## Filter out to only get the tweets that were published yesterday
    date_tweet_df = get_tweets_for_date(search_tweet_df, yesterday_date)

    # Now process the output
    if content_type == 'Medium':
        content_tweet_df = cleanup_medium_tweets(date_tweet_df, num_posts)

    if content_type == 'Substack':
        content_tweet_df = cleanup_substack_tweets(date_tweet_df, num_posts)

    if content_type == 'Blog':
        content_tweet_df = cleanup_blog_tweets(date_tweet_df, num_posts)
    print()
    
    return content_tweet_df

def save_to_medium_collection(medium_df):
    medium_collection = db.medium_collection
    cur = medium_collection.find() 
    print('We have %s medium entries at the start' % cur.count())
    
    medium_ids=list(medium_collection.find({},{ "_id": 0, "id": 1})) 
    medium_ids=list((val for dic in medium_ids for val in dic.values()))
    
    for dfs in medium_df.to_dict('records'):
        if dfs['id'] not in medium_ids:
            medium_collection.insert_one(dfs)
            
    cur = medium_collection.find() 
    print('We had %s medium entries at the start' % cur.count())



def save_to_blog_collection(blog_df):
    blog_collection = db.blog_collection
    cur = blog_collection.find() 
    print('We have %s blog entries at the start' % cur.count())
        
    blog_ids=list(blog_collection.find({},{ "_id": 0, "id": 1})) 
    blog_ids=list((val for dic in blog_ids for val in dic.values()))
    
    for dfs in blog_df.to_dict('records'):
        if dfs['id'] not in blog_ids:
            blog_collection.insert_one(dfs)
            
    cur = blog_collection.find()
    print('We had %s blog entries at the start' % cur.count())


def save_to_substack_collection(substack_df):
    substack_collection = db.substack_collection
    cur = substack_collection.find() 
    print('We have %s substack entries at the start' % cur.count())
    
    
    substack_ids=list(substack_collection.find({},{ "_id": 0, "id": 1})) 
    substack_ids=list((val for dic in substack_ids for val in dic.values()))
    
    for dfs in substack_df.to_dict('records'):
        if dfs['id'] not in substack_ids:
            substack_collection.insert_one(dfs)
            
    cur = substack_collection.find()
    print('We had %s substack entries at the start' % cur.count())




def get_latest_article_tweets(content_type_search_dict, num_tweets, num_posts, yesterday_date):
    """
    This function loops through search queries for medium, substack and blogs.
    ** Its important to note that for now we are only processing tweets that were in english.
    ** This pipeline is to run at 1am everyday and process all the tweets from the previous day so that 
       by the following morning the guys can hit the ground running hard with it. Later on we may decide to
       run the pipeline periodically so its more current but a 24 hour lag is definitely not bad at all.
    ** The function returns a dictionary containing dataframes of 'num_post' tweet for each content type.
    """
    print('We are now getting the tweets from yesterday related to blogs/articles')
    content_tweet_list = []
    for content_type in content_type_search_dict:
        try:
            start = time.time()
            print(content_type)
            search_list = content_type_search_dict[content_type]
        
            content_tweet_df = process_tweets_from_content(content_type, search_list, num_tweets, num_posts, yesterday_date)
            content_tweet_df['content_type'] = content_type
            print(content_tweet_df)

            substack_df = content_tweet_df[content_tweet_df['content_type'] == 'Substack']
            medium_df = content_tweet_df[content_tweet_df['content_type'] == 'Medium']
            blog_df = content_tweet_df[content_tweet_df['content_type'] == 'Blog']

            save_to_medium_collection(medium_df)
            save_to_substack_collection(substack_df)
            save_to_blog_collection(blog_df)
        except:
            pass



def run_the_process():
    get_latest_article_tweets(content_type_search_dict, num_tweets, num_posts, yesterday_date)