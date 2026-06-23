function e(n){return n?n.split(`
`).map(r=>{const t=r.indexOf(":");return t===-1?null:{key:r.substring(0,t).trim(),value:r.substring(t+1).trim()}}).filter(Boolean):[]}export{e as p};
