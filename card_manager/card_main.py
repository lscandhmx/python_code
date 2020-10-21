import card_manager.card_tool as card_tool



if __name__ == '__main__':

    while True:
        # 显示功能菜单
        card_tool.show_menu()

        action_str = input("请选择操作功能：")

        if action_str in ['1', '2', '3']:
            # 1新建名片
            if action_str == '1':
                card_tool.create_card()
            # 2显示全部
            elif action_str == '2':
                card_tool.show_all()
            # 3查询名片
            elif action_str == '3':
                card_tool.search_card()
        elif action_str == '0':
            print('退出系统,欢迎下次使用')
            break
        else:
            print("您输入的不正确，请重新选择")



