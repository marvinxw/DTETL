# coding=utf-8
import re
from dwetl.petl.p_decorator import check_params


Links = {
    '^#card_detail$': 1,
    '^#products$': 1,
    '^#coupons$': 1,
    '^#pointShop$': 1,
    '^#profile$': 1,
    '^#home$': 1,
    '^#productDetail/[1-9]+$': 6,
    '^#couponDetail/[1-9]+$': 2,
    '^#pointDetail/[1-9]+$': 7,
    '^#flowerDetail/[1-9]+$': 8,
    '^#news$': 1,
    '^#newsDetail/[1-9]+$': 9,
    '^#serviceDetail/[1-9]+$': 1,
    '^#notificationCenter$': 1,
    '^#login$': 1,
    '^#register$': 1,
    '^#registerProfile$': 1,
    '^#forgetPassword$': 1,
    '^#profileEdit$': 1,
    '^#feedback$': 1,
    '^#flower$': 1,
    '^#redenv$': 1,
    '^#shopLocation$': 1,
    '^#shopLocation/[1-9]+$': 1,
    '^#survey$': 1,
    '^#serviceDetail/about$': 1,
    '^#serviceDetail/serviceTerm$': 1,
    '^#serviceDetail/helpCenter$': 1,
    '^#serviceDetail/bindCard$': 1,
    '^#serviceDetail/shopRecord$': 1,
    '^#serviceDetail/pointUseRecord$': 1,
    '^#serviceDetail/electronicCoupons$': 1,
    '^#serviceDetail/bindPay$': 1
}


@check_params(str)
def _object_id(content):
    try:
        object_id = int(content.split('/')[-1])
    except:
        object_id = 0

    return object_id


@check_params(str)
def _object_id_2(content=None):

    return content


@check_params(str)
def _interaction_type(content):

    interaction_type = 1
    for route in Links.keys():
        p = re.compile(route)
        if p.match(content):
            interaction_type = Links[route]

    return interaction_type


@check_params(str)
def _user_id(userid):
    try:
        user_id = int(userid)
    except:
        user_id = 0

    return user_id

