var url = require('url')
    ,http = require('http')
	,zlib = require('zlib')
	,querystring = require('querystring')
	,stream = require('stream')
	,util = require('util')
	,emitter = require('events').EventEmitter

var Writable = stream.Writable
	,Transform = stream.Transform
	,extend = util._extend

var settings = require('./config/setting.json')


var CACHEEND = settings.CACHEEND
	,ONPORT = settings.ONPORT 	
	,DSMAP = settings.DSMAP

var ENABLEGZIP = settings.ENABLEGZIP || false
	,TRANSINGZIP = settings.TRANSINGZIP || 'A'

//var ENABLEGZIP = false  //可以返给浏览器 gzip ，关闭后可以由 nginx 来做
//	,TRANSINGZIP = 'A' //Y N A , 调用后端接口使用 gzip 传输，带宽压力不大的话可以关闭

var spChar = '::'


var acceptor = http.createServer(gRoute).listen(ONPORT)

//http.globalAgent.maxSockets = 25
//http://qzaidi.github.io/2013/07/20/surprises/

//添加 callback 前后段
function transRes( opt){
	this.opt = opt || {}
	this.buf = opt.startStr && new Buffer(opt.startStr)  
	this.holdToWrap = opt.multi || opt.outputCompType
	Transform.call(this)
}

util.inherits(transRes , Transform)

transRes.prototype._transform = function (chunk, encoding, done) {
	if (this.holdToWrap) 
		this.buf = Buffer.concat([this.buf , chunk])
	else{
		if (this.buf) {
			this.push(this.buf)
			this.buf = null
		}
		this.push(chunk)
	}
	
	done()
}

transRes.prototype._flush  = function(done){
	if (this.holdToWrap) this.push(this.buf)
	if (this.opt.endStr) this.push(this.opt.endStr)

	done()
}

//暴露end方法
function nopStream(){
	this.readable = true
	this.writable = true
}
util.inherits(nopStream, stream)
nopStream.prototype.write = function (chunk) {
	this.emit.call(this ,'data' , chunk)
}
nopStream.prototype.end = function (chunk) {
	chunk && this.emit.call(this ,'data' , chunk)
	this.emit.call(this ,'end' )
}



//合并输出多请求
function multiStream(opt) {
	this.opt = opt || {}
	this.readable = true
	this.writable = true
};

util.inherits(multiStream, stream)

multiStream.prototype.write = function (chunk) {
	this.opt.buf = Buffer.concat([this.opt.buf , chunk])
}

multiStream.prototype.end = function (chunk) {
	if (chunk) this.write(chunk)
	if (--this.opt.count <=0 ){
		this.emit.call(this ,'data' , this.opt.buf)
		this.emit.call(this ,'end' )
		this.opt.buf = null
	}else {
		this.write(new Buffer(';\n\n'))
		//this.opt.buf = Buffer.concat([this.opt.buf , new Buffer(';\n')])
	}
}

function errorLog(url , err){
	console.log(url , err)
}

function getTTL(ttl){
	if (!ttl) return 0
	var val = ttl.slice(0 , -1) * 1
		,unit = ttl.slice(-1)
	if (!val) return 0
	var ratio = {
		'm' : 60
		,'h' : 3600
		,'d' : 3600 * 24
		}[unit]

	if (ratio) val *= ratio
	return val
}

function sortQuery(o){
	if (!o) return '' 
	var newO = {}
	Object.keys(o).sort().forEach(function(key){
		newO[key] = o[key]
	})
	return newO 
}

function gRoute(req , res){
	//数据源::缓存有效期::模块::接口地址::callback::options::请求参数?userid=123|access_token=123
	//a0::3m::snake::/welcome/::callback::{}::a=1&b=2?userid=123&a=1&b=3
	
	//delete req.headers['accept-encoding']
	
	

	console.log('req ' + req.url)
	req.pause()
	var options = url.parse(req.url )
	
	options.method = req.method
	options.headers = req.headers
	options.agent = false
	
	switch (TRANSINGZIP){
		case 'Y': 
			options.headers['accept-encoding'] = 'gzip'
			break	
		case 'N': 
			delete options.headers['accept-encoding'] 
			break	
	}

	var backEnd = options.pathname.slice(1).split(spChar + '+' + spChar)
		,query = options.query

	var outputCompType = false
	if (ENABLEGZIP && req.headers['accept-encoding']  && req.headers['accept-encoding'].indexOf('gzip') >= 0){
		outputCompType = 'gzip' 
	}
	var pipeOpt = {
				'endStr' : ')'
				,'outputCompType'  : outputCompType 
				}

	if (backEnd.length > 1) {
		pipeOpt.count =  backEnd.length
		pipeOpt.multi = true
		pipeOpt.buf = new Buffer(0)
	}
	
	backEnd.forEach(pullBcknd.bind(null ,req , res , options , pipeOpt))
	//backEnd.forEach(pullBcknd)

	req.resume()
}
	
	

function pullBcknd(req , res , options , pipeOpt ,oriBkReq){
	bkReq = oriBkReq.split(spChar , 6)

	var  ttl = getTTL(bkReq[1])
		,source = bkReq[2]
		,url = bkReq[3]
		,callBack = bkReq[4]
		,reqOpts = bkReq[5] ? querystring.parse(bkReq[5]) :  {}
		,params = bkReq.length >=6 ? oriBkReq.slice(bkReq.join(spChar).length + spChar.length   ) : ''
		,query = options.query
		,outputCompType = pipeOpt.outputCompType


	var reqHost = DSMAP[source || 'snake'] //snake doota etc...
	if (!reqHost) {
		errorLog(oriBkReq , source + ' is not configed')
		return res.end(source + ' is not configed')	
	}
	if (ttl && CACHEEND.HOST){
		//should be cache
		options.host = CACHEEND.HOST 
		options.port = CACHEEND.PORT 
	}else{
		//options.headers.nocache = 1
		options.host = reqHost.host
		options.port = reqHost.port || 80
	}

	reqHost = reqHost.host + (reqHost.port ? (':' + reqHost.port) : '')
	options.headers.host = reqHost
	options.headers.reqTime = + new Date


	if (params && query) {
		params = querystring.parse(params)
		params = extend( querystring.parse(query) , params)
	} else {
		params = querystring.parse(params || query)		
	}
	params = querystring.stringify(sortQuery(params))

	if (params) url += '?' + params
	options.path = 'http://' + reqHost + url 
	///console.log(options)

	var callBackBgn = callBack && ( callBack = 'window.' + callBack + ' && ' + callBack +  '(' ) 
	var fullReqUrl = options.path

	var signPipe  
		,multiPipe
	if (pipeOpt.multi) multiPipe = new multiStream(pipeOpt)

	var connector = http.request(options, function(srvRes) {
		var deCompress 
			,doCompress
			,backHead = srvRes.headers
		///console.log(backHead)
		srvRes.pause()
		
		signPipe = new nopStream 

		if ('gzip' == backHead['content-encoding']) {
			deCompress = zlib.createGunzip()
		}

		var toWrapOut = !!callBackBgn
		if ('gzip' == outputCompType ){
			backHead['content-encoding'] = outputCompType 
			doCompress = zlib.createGzip()
		}else {
			delete backHead['content-encoding'] 
		}

		delete backHead['content-length']

		res.setHeader(  'Content-Type' ,  'application/javascript;; charset=UTF-8' )
		res.writeHeader(srvRes.statusCode, backHead)

	//	if (callBackBgn && !pipeOpt.multi && !outputCompType) res.write( callBackBgn)
		var pipeObj = srvRes.pipe(signPipe)
		if (deCompress) pipeObj = pipeObj.pipe(deCompress)
		if (callBackBgn) {
			pipeOpt.startStr = callBackBgn
			pipeObj = pipeObj.pipe(new transRes( pipeOpt))	
		}
		
		if (pipeOpt.multi) pipeObj = pipeObj.pipe(multiPipe)
		if (doCompress) pipeObj = pipeObj.pipe(doCompress)
	
		pipeObj.pipe(res)

		srvRes.resume()

		if ( ttl && backHead.age*1 > ttl ){
			purge(fullReqUrl)		
		}
	})
	function outputError(err){
		///how to push into pipe 
		err = '/*[' + fullReqUrl + '] [' + err + ']*/'
		if (signPipe) 
			signPipe.end(err)
		else if (multiPipe)
			multiPipe.end(err)
		else {
			if (callBackBgn) err = callBackBgn + err + pipeOpt.endStr
			res.end(err)
		}
	}

	connector.setTimeout(CACHEEND.TIMEOUT , function(){
		errorLog(fullReqUrl , 'timeout')
		outputError('timeout')
		connector.abort()
	})

	connector.on('error' , function(err){
		//if abort request when timeout ,it trigger  socket hang up
		errorLog(fullReqUrl , err)
		outputError(err.code)
	})

	req.pipe(connector)
}

function purge(url){
	console.log('purge' , url)
	var purOpt = {
		 agent : false
		,method : 'PURGE'
		,host : CACHEEND.HOST
		,port : CACHEEND.PORT
		,path : url 
	}
	var pureAct = http.request(purOpt , function(purRes){
		console.dir( url)	
	})
	pureAct.on('error' , function(err){
		console.log(err)
	})
	pureAct.end()
}

settings.TESTPORT && http.createServer(function (req, res) {
	//backend should behind varnish which  cache anything by url
	console.log('9100' , req.url)
	
    res.setHeader('hitime' , +new Date)

	var zlib = require('zlib')
	var output = JSON.stringify(req.headers, true, 2)
    //res.setHeader('content-encoding' , 'gzip')
    res.setHeader('Transfer-Encoding' , 'chunked')
	
    //res.setHeader('content-length' , output.length)

	/*	
	zlib.gzip(output , function(err , result){
		res.setHeader('content-length' , result.length)
		res.writeHead(200, { 'Content-Type': 'text/plain' })
		res.end(result)
	})
	*/
	
	res.write(output)
	res.end()
}).listen(settings.TESTPORT)
