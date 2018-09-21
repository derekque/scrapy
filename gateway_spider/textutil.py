import re

def filtertext(text):
    # replace script
    text1 = re.sub(r'<script(.|\n)+?</script>', '', text)
    # replace style css
    text2 = re.sub(r'<style(.|\n)+?</style>', '', text1)
    # replace tag
    text3 = re.sub(r'<.+?>|\s*', ' ', text2)
    # text4 = re.sub('[\^\\u4e00-\\u9fa5]+','',text3)
    text4 = re.sub('[\^\\x00-\\xff]+', '', text3)
    return text4
