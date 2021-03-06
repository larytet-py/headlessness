# Headless Chrome


## Usage

```
black src/. && docker build --build-arg CACHEBUST=$(date +%s) -t chrome .
```

Check a single URL
```
docker run --rm -p 5900:5900 -e REPORT_TYPE="json" -e HEADLESS="true" -e TIMEOUT="30.0" -e REQUEST_ID="1" -e URL="https://www.w3schools.com/" chrome 
```
or start the service
```
docker run --rm -p 5900:5900 -p 8081:8081 -e TIMEOUT="30.0" -e HEADLESS="false" chrome 
curl --silent -X POST "http://0.0.0.0:8081/fetch?url=https%3A%2F%2Fwww.w3schools.com%2F&transaction_id=1"
```

Try VNC 127.0.0.1:5900
```
remmina -c $PWD/local-chrome.remmina
```

Stats
```
while [ 1 ];do echo -en "\\033[0;0H";curl "http://0.0.0.0:8081/stats?format=text";sleep 0.2;done
```

## Links

* https://github.com/dhamaniasad/HeadlessBrowsers - Collection of links
* http://www.smartjava.org/content/using-puppeteer-in-docker/
* https://blog.logrocket.com/how-to-set-up-a-headless-chrome-node-js-server-in-docker/
* https://paul.kinlan.me/hosting-puppeteer-in-a-docker-container/
* https://vsupalov.com/headless-chrome-puppeteer-docker/
* https://github.com/puppeteer/puppeteer
* https://github.com/buildkite/docker-puppeteer
* https://developers.google.com/web/updates/2017/04/headless-chrome
* https://chromium.googlesource.com/chromium/src/+/lkgr/headless/README.md
* https://github.com/Zenika/alpine-chrome#image-disk-size
* https://playwright.dev/#
* https://blog.logrocket.com/playwright-vs-puppeteer/
* https://github.com/puppeteer/puppeteer/blob/main/docs/troubleshooting.md#setting-up-chrome-linux-sandbox
* https://github.com/mafredri/cdp
* https://playwright.dev/#version=v1.2.1&path=docs%2Fcore-concepts.md&q=
* https://github.com/chromedp/chromedp
* https://duo.com/decipher/driving-headless-chrome-with-python
* https://chromedevtools.github.io/devtools-protocol/    - a list of tools
* https://github.com/ChromeDevTools/awesome-chrome-devtools
* https://github.com/hyperiongray/trio-chrome-devtools-protocol
* https://github.com/chazkii/chromewhip
* https://github.com/go-rod/rod/tree/master/lib/examples/compare-chromedp
* https://pkg.go.dev/github.com/wirepair/gcd/v2/gcdapi#Fetch.Enable  - an API?
* https://github.com/wirepair/gcd
* https://github.com/pyppeteer/pyppeteer
* https://w3c.github.io/web-performance/specs/HAR/Overview.html
* https://github.com/nbasker/tools/blob/master/asyncioeval/ticker.py
* https://aiomultiprocess.omnilib.dev/en/stable/
