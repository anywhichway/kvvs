(function() {
	"use strict"
	
	const fs = require("fs"),
		path = require("path"),
		jssha3 = require("js-sha3"),
		{promisify} = require("util");
		
	fs.readAsync = promisify(fs.read);
	fs.readFileAsync = promisify(fs.readFile);
	fs.writeFileAsync = promisify(fs.writeFile);
	
	const ensureDir = path => {
			const parts = path.split("/");
			let dir = parts.shift();
			while(parts.length>1) {
				dir += "/" + parts.shift();
				try {
					fs.mkdirSync(dir);
				} catch(e) {
					if(e.code!=="EEXIST") throw e;
				}
			}
		},
		rmdirSync = dir => {
			const list = fs.readdirSync(dir);
			for(const name of list) {
				const filename = path.join(dir, name);
				if(filename == "." || filename == "..") {
					// pass these files
				} else if(fs.statSync(filename).isDirectory()) {
					rmdirSync(filename);
				} else {
					fs.unlinkSync(filename);
				}
			}
			fs.rmdirSync(dir);
		};
	
	class KVVS {
		constructor(path,options) {
			this.log = new AppendLog(path,options);
		}
		async clear() {
			return this.log.clear();
		}
		count() {
			return this.log.count();
		}
		async getHistory(key,test = () => true) {
			return this.log.getHistory(key,test);
		}
		async getItem(key,testOrVersion,raw) {
			const record = await this.log.get(key,testOrVersion);
			!raw || Object.assign(raw,result);
			return record ? record.value : undefined;
		}
		async setItem(key,value,metadata) {
			this.log.set(key,value,metadata);
		}
		async removeItem(key,metadata) {
			this.log.set(key,undefined,metadata);
		}
	}
	
	class AppendLog {
		constructor(path,options) {
			this.dir = path;
			this.options = Object.assign({},options);
			if(!this.options.optimize) this.options.optimize="s";
			if(!this.options.cacheMax) this.options.cacheMax = 100000;
			if(!this.options.cacheStep) this.options.cacheStep = .1;
			if(this.options.hash===undefined) this.options.hash = true;
			this.init();
		}
		adjustCache() {
			if(this.cacheCount>=this.options.cacheMax) {
				let count = this.options.cacheStep < 1 ? this.options.cacheMax * this.options.cacheStep : this.options.cacheStep;
				for(const key in this.keys) {
					delete this.keys[key];
					count--;
					this.cacheCount--;
					if(count<=0) break;
				}
			}
		}
		clear() {
			if(this.keyfd) fs.closeSync(this.keyfd);
			fs.closeSync(this.valuefd);
			delete this.keys;
			rmdirSync(this.dir);
			this.init();
		}
		count() {
			if(this.options.optimize==="s") {
				return Object.keys(this.keys).length;
			}
			return fs.readdirSync(this.dir).length-1;
		}
		async get(key,testOrVersion) {
			const hash = this.options.hash ? jssha3.sha3_256(key) : key,
					path = this.dir + "/" + hash + ".json";
			let pointer = this.keys[hash];
			if(!pointer && this.options.optimize!=="s") {
				try {
					pointer = JSON.parse(await fs.readFileAsync(path,"utf8"));
					this.adjustCache();
					this.keys[hash] = pointer;
					this.cacheCount++;
				} catch(e) {
					return;
				}
			}
			if(!pointer) return;
			let buf = Buffer.alloc(pointer.length);
			await fs.readAsync(this.valuefd,buf,0,pointer.length,pointer.start);
			const test = testOrVersion===undefined ? () => true : typeof(testOrVersion)==="number" ? record => record.sequence===testOrVersion : testOrVersion;
			let record = JSON.parse(buf.toString());
			while(record.previous && record.sequence>0 && !test(record)) {
				buf = Buffer.alloc(record.previous.length);
				await fs.readAsync(this.valuefd,buf,0,record.previous.length,record.previous.start);
				record = JSON.parse(buf.toString());
			}
			return test(record) ? record : undefined;
		}
		init() {
			const parts = this.dir.split("/");
			let dir = parts.shift();
			while(parts.length>0) {
				try {
					fs.mkdirSync(dir);
				} catch(e) {
					if(e.code!=="EEXIST") throw e;
				}
				dir = dir + "/" + parts.shift();
			}
			try {
				fs.mkdirSync(dir);
			} catch(e) {
				if(e.code!=="EEXIST") throw e;
			}
			const keyPath = this.dir + "/keys.json";
			this.valuefd = fs.openSync(this.dir + "/values.json","a+");
			this.valuestat = fs.fstatSync(this.valuefd);
			try { // will read for speed optimized or read for conversion to memory optimized
				this.keys = JSON.parse(`{${fs.readFileSync(keyPath,"utf8")||""}}`);
			} catch(e) {
				if(e.code!=="ENOENT") throw e;
				this.keys = {};
			}
			this.cachecount = Object.keys(this.keys).length;
			if(this.options.optimize==="s") { // compress the keys by saving again
				let first = true,
					data = "";
				for(const key in this.keys) {
					data += `${first ? "" : ","}"${key}":${JSON.stringify(this.keys[key])}`;
					first = false;
				}
				fs.writeFileSync(keyPath,data,"utf8");
				this.keyfd = fs.openSync(keyPath,"a+");
				this.keystat = fs.fstatSync(this.keyfd);
			} else { // convert keys if necessary
				for(const key in this.keys) {
					const path = this.dir + "/" + key + ".json";
					ensureDir(path);
					fs.writeFileSync(path,JSON.stringify(this.keys[key]),"utf8");
				}
				try { // key file is not needed for memory optimized
					fs.unlinkSync(keyPath);
				} catch(e) {
					if(e.code!=="ENOENT") throw e;
				}
			}
		}
		set(key,value,metadata) {
			const hash = this.options.hash ? jssha3.sha3_256(key) : key,
				path = this.dir + "/" + hash + ".json";
			let previous = this.keys[hash],
				vdata,
				pointer,
				kdata;
			if(!previous) {
				this.cacheCount++;
				if(this.options.optimize!=="s") {
					try {
						previous = JSON.parse(fs.readFileSync(path,"utf8"));
					} catch(e) {
						if(e.code!=="ENOENT") throw e;
					}
				}
			}
			const sequence = previous ? previous.sequence + 1 : 0,
				timestamp = Date.now(),
				record = {value,timestamp,sequence,previous};
			vdata = JSON.stringify(record);
			pointer = Object.assign({start:this.valuestat.size,length:vdata.length,sequence},metadata);
			kdata = JSON.stringify(pointer);
			fs.writeSync(this.valuefd,vdata);
			if(this.options.optimize==="s") {
				fs.writeSync(this.keyfd,`${this.keystat.size > 0 ? "," : ""}"${hash}":${kdata}`);
				this.keystat.size += this.keystat.size > 0 ? kdata.length : kdata.length + 1;
			} else {
				ensureDir(path);
				fs.writeFileSync(path,kdata,"utf8");
				this.adjustCache();
			}
			this.keys[hash] = pointer;
			this.valuestat.size += vdata.length;
		}
		truncate(version,keyOrTest) { // drop any versions earlier than version and optionals matching key or where pass test, no args just keeps most recent version of all
			// write everything to a new file
			// update keys
		}
	}
		
/*async function test() {	
	const count = 100,
		data = "".padStart(1024,"0");

	const ap = new AppendLog("data2",{optimize:"m"});
	await ap.set("person",{name:"Joe"});
	await ap.set("person",{name:"Bill"});
	console.log(await ap.get("person"));
	console.log(await ap.get("person",1));
	console.log(await ap.get("person",0));
	
	
	let start = Date.now();
	for(let i=0;i<count;i++) {
		await ap.set("object/"+i,data);
	}
	console.log("Set Rec Sec:",count/((Date.now()-start)/1000));
	start = Date.now();
	for(let i=0;i<count;i++) {
		await ap.get("object/"+i);
	}
	console.log("Get Rec Sec:",count/((Date.now()-start)/1000));
	console.log(ap.count());
	ap.clear();
	
	let kvvs = new KVVS("data3",{optimize:"m"});
	start = Date.now();
	for(let i=0;i<count;i++) {
		await kvvs.setItem("object/"+i,data);
	}
	console.log("Set Rec Sec:",count/((Date.now()-start)/1000));
	start = Date.now();
	for(let i=0;i<count;i++) {
		await kvvs.getItem("object/"+i);
	}
	console.log("Get Rec Sec:",count/((Date.now()-start)/1000));
	console.log(kvvs.count());
	kvvs.clear();
	
	kvvs = new KVVS("data4",{optimize:"s"});
	start = Date.now();
	for(let i=0;i<count;i++) {
		await kvvs.setItem("object/"+i,data);
	}
	console.log("Set Rec Sec:",count/((Date.now()-start)/1000));
	start = Date.now();
	for(let i=0;i<count;i++) {
		await kvvs.getItem("object/"+i);
	}
	console.log("Get Rec Sec:",count/((Date.now()-start)/1000));
	console.log(kvvs.count());
	kvvs.clear();
}
test();*/
	
module.exports = KVVS;
	
}())