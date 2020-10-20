import card_manager.card_tool as card_tool

card_info = []

if __name__ == '__main__':

    while True:
        # 显示功能菜单
        print('*' * 50)
        print("欢迎使用【名片管理系统】V1.0")
        print("1.新建名片")
        print("2.显示全部")
        print("3.查询名片")
        print()
        print("0.退出系统")
        print('*' * 50)

        action_str = input("请选择操作功能：")

        if action_str in ['1', '2', '3']:
            # 1新建名片
            if action_str == '1':
                card_tool.create_card(card_info)
            # 2显示全部
            elif action_str == '2':
                card_tool.show_card(card_info)
            # 3查询名片
            elif action_str == '3':
                print(3)
        elif action_str == '0':
            print('退出系统,欢迎下次使用')
            break
        else:
            print("您输入的不正确，请重新选择")



