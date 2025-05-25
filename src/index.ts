import { createClient } from '@clickhouse/client'
import TelegramBot from 'node-telegram-bot-api'
import * as dotenv from 'dotenv'

dotenv.config()

const clickhouseConfig = [
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_1_HOST,
      port: process.env.CLICKHOUSE_1_PORT,
      username: process.env.CLICKHOUSE_1_USER,
      password: process.env.CLICKHOUSE_1_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_2_HOST,
      port: process.env.CLICKHOUSE_2_PORT,
      username: process.env.CLICKHOUSE_2_USER,
      password: process.env.CLICKHOUSE_2_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_3_HOST,
      port: process.env.CLICKHOUSE_3_PORT,
      username: process.env.CLICKHOUSE_3_USER,
      password: process.env.CLICKHOUSE_3_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_4_HOST,
      port: process.env.CLICKHOUSE_4_PORT,
      username: process.env.CLICKHOUSE_4_USER,
      password: process.env.CLICKHOUSE_4_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_5_HOST,
      port: process.env.CLICKHOUSE_5_PORT,
      username: process.env.CLICKHOUSE_5_USER,
      password: process.env.CLICKHOUSE_5_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_6_HOST,
      port: process.env.CLICKHOUSE_6_PORT,
      username: process.env.CLICKHOUSE_6_USER,
      password: process.env.CLICKHOUSE_6_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_7_HOST,
      port: process.env.CLICKHOUSE_7_PORT,
      username: process.env.CLICKHOUSE_7_USER,
      password: process.env.CLICKHOUSE_7_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_8_HOST,
      port: process.env.CLICKHOUSE_8_PORT,
      username: process.env.CLICKHOUSE_8_USER,
      password: process.env.CLICKHOUSE_8_PASSWORD,
    },
    {
      name: process.env.NAME,
      host: process.env.CLICKHOUSE_9_HOST,
      port: process.env.CLICKHOUSE_9_PORT,
      username: process.env.CLICKHOUSE_9_USER,
      password: process.env.CLICKHOUSE_9_PASSWORD,
    },

];


const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN!;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID!;


const bot = new TelegramBot(TELEGRAM_BOT_TOKEN, {polling: true})



const checkUpdates = async () => {
    for (const config of clickhouseConfig){
        const client = createClient({
            url: `http://${config.host}:${config.port}`,
            username: config.username,
            password: config.password,
        });

        try {
            console.log(`Checking ${config.name}...`)



            const analyticsResult = await (await client.query({
                query: `
                  SELECT MAX(timestamp) AS last_update
                  FROM analytics
                `,
                format: 'JSONEachRow',
              })).json<{last_update: string}>();


              const analyticsDetailedResult = await(await client.query({
                query: `SELECT MAX(timestamp) AS last_update FROM analyticsDetailed`,
                format: "JSONEachRow"
              })).json<{last_update:string}>()

              console.log(`Server ${config.host}`)

              console.log(analyticsResult)
              console.log(analyticsDetailedResult)

              try{
                const currentTimestamp = new Date().getTime()
                const analyticsResultTimestamp = new Date(analyticsResult[0].last_update).getTime()
                const analyticsDetailedResultTimestamp = new Date().getTime()
  
                const analyticsResultDiff = currentTimestamp - analyticsResultTimestamp;
                const analyticsDetailedResultDiff = currentTimestamp - analyticsDetailedResultTimestamp;
  
                console.log(analyticsResultTimestamp)
                console.log(currentTimestamp)

                if (analyticsResultDiff > 1000 * 60 * 120){
                  console.log(`Analytics data is not up to date for ${config.name}`)
                  bot.sendMessage(TELEGRAM_CHAT_ID, `Analytics data is not up to date for ${config.name}`)
                }
  
                if (analyticsDetailedResultDiff > 1000 * 60 * 60){
                  bot.sendMessage(TELEGRAM_CHAT_ID, `Detailed analytics data is not up to date for ${config.name}`)
                }
                
              }catch (err){
                console.log(err)
                bot.sendMessage(TELEGRAM_CHAT_ID, `Error while checking ${config.name}`)
              }
              





        } catch (err){
            console.log(err)
        }
    }
}

let messageID = 0;

checkUpdates()

setInterval(() => {
    checkUpdates()

    if (messageID > 0){
        bot.deleteMessage(TELEGRAM_CHAT_ID, messageID)
    }

    bot.sendMessage(TELEGRAM_CHAT_ID, `Updated at ${new Date().toISOString()}`).then((msg) => {
        messageID = msg.message_id
    })
}, 900000)

bot.sendMessage(TELEGRAM_CHAT_ID, `Bot version of 1.0.0 is running...`)