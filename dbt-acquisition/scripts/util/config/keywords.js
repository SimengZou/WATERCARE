// https://docs.snowflake.com/en/sql-reference/reserved-keywords

let selector = '#reserved-limited-keywords > div > div > div.scrollable-table-scroll-container > table > tbody'
console.log('RESERVED_KEYWORDS = ' + JSON.stringify(
    [...document.querySelector(selector).getElementsByTagName('tr')]
        .filter(tr => !tr.getElementsByTagName('strong').length)
        .map(tr => tr.getElementsByTagName('td')[0].innerText),
    null, 2)
);
