conn = sqlite('pend.db','readonly')

stocks = fetch(conn,'SELECT symbol,sector,dayreturn FROM stockDT JOIN stockTable LIMIT 10')
