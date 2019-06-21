# node-query-builder

[![CircleCI](https://circleci.com/gh/Commander-lol/node-query-builder/tree/master.svg?style=svg)](https://circleci.com/gh/Commander-lol/node-query-builder/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/Commander-lol/node-query-builder/badge.svg?branch=master)](https://coveralls.io/github/Commander-lol/node-query-builder?branch=master) 

A simple type based query builder for Postgresql queries

## Installation

`npm i @commander-lol/pg-query`

## Usage

```js
const QB = require('@commander-lol/pg-query')
const builder = new QB()

const sql = builder.table('users')
	.where('deleted_at', QB.Null(), 'IS')
	.select('name', 'email', 'password')
```
