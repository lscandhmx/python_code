# 记录所有名片字典
card_list = []

def show_menu():

    """显示菜单"""
    print('*' * 50)
    print("欢迎使用【名片管理系统】V1.0")
    print("1.新建名片")
    print("2.显示全部")
    print("3.查询名片")
    print()
    print("0.退出系统")
    print('*' * 50)


def create_card():

    """新增名片"""
    print('-' * 50)
    print("新增名片")

    name = input("请输入姓名：")
    phone = input("请输入电话：")
    qq = input("请输入QQ:")
    email = input("请输入邮箱：")

    card_dict = {"name": name,
                 "phone": phone,
                 "qq": qq,
                 "email": email}

    card_list.append(card_dict)

    print(card_dict)
    print("添加%s成功" % name)


def show_all():

    """显示所有名片"""
    print('-' * 50)
    print("显示名片")
    print()

def search_card():

    """查询名片"""
    print('-' * 50)
    print("查询名片")
