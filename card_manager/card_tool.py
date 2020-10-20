def create_card(card_info):
    card_personal_info = {}
    card_personal_info['name'] = '张三'
    card_personal_info['age'] = '17'
    card_info.append(card_personal_info)

def show_card(card_info):
    print(card_info)