# gomem

[![GoDoc](https://godoc.org/github.com/gomem/gomem?status.svg)](https://godoc.org/github.com/gomem/gomem)
[![CircleCI](https://circleci.com/gh/gomem/gomem.svg?style=svg)](https://circleci.com/gh/gomem/gomem)

Various packages for working with Apache Arrow in Go.

Included in gomem is a DataFrame implementation. It uses
[Apache Arrow](https://github.com/apache/arrow/tree/master/go)
under the hood to store an manipulate data in a columnar format.

## Packages

| Tables                  | Description                                                            | Link                  |
| ----------------------- | ---------------------------------------------------------------------- | --------------------- |
| [dataframe](#dataframe) | A DataFrame implementation using Arrow.                                | [code](dataframe/)    |
| collection              | Abstract access to Arrow arrays using gomem Objects.                   | [code](collection/)   |
| iterator                | Iterators for iterating over Arrow arrays.                             | [code](iterator/)     |
| logical                 | Abstract logical types.                                                | [code](logical/)      |
| object                  | Abstract object type capable of automatically converting Object types. | [code](object/)       |
| smartbuilder            | Abstract Arrow array builder.                                          | [code](smartbuilder/) |

---

## dataframe

A DataFrame built on [Apache Arrow](https://github.com/apache/arrow/tree/master/go).

<!-- ----------------------------------------------------------------------------------------------- -->

### Installation

Add the package to your `go.mod` file:

    require github.com/gomem/gomem master

Or, clone the repository:

    git clone --branch master https://github.com/gomem/gomem.git $GOPATH/src/github.com/gomem/gomem

A complete example:

```bash
mkdir my-dataframe-app && cd my-dataframe-app

cat > go.mod <<-END
  module my-dataframe-app

  require github.com/gomem/gomem master
END

cat > main.go <<-END
  package main

  import (
    "fmt"

    "github.com/apache/arrow/go/arrow/memory"
    "github.com/gomem/gomem/pkg/dataframe
  )

  func main() {
    pool := memory.NewGoAllocator()
    df, _ := dataframe.NewDataFrameFromMem(pool, dataframe.Dict{
      "col1": []int32{1, 2, 3, 4, 5},
      "col2": []float64{1.1, 2.2, 3.3, 4.4, 5},
      "col3": []string{"foo", "bar", "ping", "", "pong"},
      "col4": []interface{}{2, 4, 6, nil, 8},
    })
    defer df.Release()
    fmt.Printf("DataFrame:\n%s\n", df.Display(0))
  }

  // DataFrame:
  // rec[0]["col1"]: [1 2 3 4 5]
  // rec[0]["col2"]: [1.1 2.2 3.3 4.4 5]
  // rec[0]["col3"]: ["foo" "bar" "ping" "" "pong"]
  // rec[0]["col4"]: [2 4 6 (null) 8]
END

go run main.go
```

---

## Arrow Array Usage

See the [DataFrame tests](dataframe/dataframe_test.go) for extensive usage examples.

## Reference Counting

From the [arrow/go README](https://github.com/apache/arrow/blob/master/go/README.md)...

> The library makes use of reference counting so that it can track when memory
> buffers are no longer used. This allows Arrow to update resource accounting,
> pool memory such and track overall memory usage as objects are created and
> released. Types expose two methods to deal with this pattern. The `Retain`
> method will increase the reference count by 1 and `Release` method will reduce
> the count by 1. Once the reference count of an object is zero, any associated
> object will be freed. `Retain` and `Release` are safe to call from multiple
> goroutines.

### When to call `Retain` / `Release`?

- If you are passed an object and wish to take ownership of it, you must call
  `Retain`. You must later pair this with a call to `Release` when you no
  longer need the object. "Taking ownership" typically means you wish to
  access the object outside the scope of the current function call.

- You own any object you create via functions whose name begins with `New` or
  `Copy` or **any operation that results in a new immutable DataFrame being returned**
  or when receiving an object over a channel. Therefore you must call
  `Release` once you no longer need the object.

- If you send an object over a channel, you must call `Retain` before sending
  it as the receiver is assumed to own the object and will later call `Release`
  when it no longer needs the object.

_Note:_ You can write a test using `memory.NewCheckedAllocator` to assert that you have
released all resources properly. See: [tests](https://github.com/gomem/gomem/blob/e0958263a91ec914aa4cd0a1b26e43aab29b4c74/dataframe/dataframe_test.go#L234)

## TODO

This DataFrame currently implements most of the scalar types we've come across.
There is still work to be done on some of the list and struct types. Feel free
to submit a PR if find you need them. This library will let you know when you do.

- [ ] Implement all Arrow DataTypes.
- [ ] Add a filter function to DataFrame.
- [ ] Add an order by function to DataFrame.

## Contributing

Pull requests are welcome!

## License

(c) 2020 Nick Poorman. Licensed under the Apache License, Version 2.0.
