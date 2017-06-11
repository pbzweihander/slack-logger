import slack_logger


def main():
    print("Logger Started!")
    logger = slack_logger.SlackLogger()
    logger.run()


if __name__ == '__main__':
    main()
