# coding=utf-8
def clean_str(s):
    s = s.replace('\r', '')
    s = s.replace('\t', '')
    s = s.replace('\f', '')
    s = s.replace('\n', '')
    s = s.replace('\"', '')
    s = s.replace('\'', '')
    s = s.replace('\v', '')
    return s



if __name__ == '__main__':
    str1=""" 支付方式:1, 现金2, 微信  
    7, 小米POS-微信      15,在线支付"""
    print(str1)
    str="""可选值['iOS', 'Android','MiniProgram','H5','Web']，区分大小写"""
    s=clean_str(str)
    print(s)