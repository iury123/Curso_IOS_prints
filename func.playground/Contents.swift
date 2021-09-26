import UIKit

precedencegroup ForwardApplication {
    associativity: left
}

infix operator =>: ForwardApplication

func => <A, B>(upstream: A, downstream: (A) -> B) -> B {
    return downstream(upstream);
}

enum CustomError: Error {
    case error(String)
}

enum EventType<T> {
    case next(T)
    case error(Error)
    case complete
}

struct EventHandler<T> {
    let onNext: (T) -> Void
    let onError: (Error) -> Void
    let onCompleted: () -> Void
}

typealias Observer<T> = (EventType<T>) -> Void
typealias Subscription = () -> Void
typealias Observable<T> = (@escaping Observer<T>) -> Subscription
typealias Operator<T,U> = (@escaping Observable<T>) -> Observable<U>

func createObservable<T>(
    _ subscribe: @escaping (_ handler: EventHandler<T>) -> Subscription
) -> Observable<T> {
    { observer in
        var tearDownSubscription: Subscription?
        var synchronous = false
        var streamClosed = false
        var unsubscribe = {}
        let handler = EventHandler<T>(
            onNext: {
                if(!streamClosed) {
                    observer(.next($0))
                }
            }, onError: {
                if(!streamClosed) {
                    synchronous = tearDownSubscription == nil
                    observer(.error($0))
                    unsubscribe()
                }
            }, onCompleted: {
                if(!streamClosed) {
                    synchronous = tearDownSubscription == nil
                    observer(.complete)
                    unsubscribe()
                }
            })
        tearDownSubscription = subscribe(handler)
        unsubscribe = {
            if(!streamClosed) {
                streamClosed = true
                tearDownSubscription?()
            }
        }
        if(synchronous) {
            unsubscribe()
        }
        return unsubscribe
    }
}


func timer(for time: Double) -> Observable<String> {
    createObservable { observer in
        let timer = Timer.scheduledTimer(withTimeInterval: time, repeats: false) {_ in
            print("TIMEOUT")
            observer.onNext("Timer fired")
            observer.onCompleted()
        }
        return { print("Timer invalidated"); timer.invalidate() }
    }
}

func map<T,U>(callback: @escaping (T) -> U) -> Operator<T, U>{
    return { upstreamObservable in createObservable { observer in
            let unsubscribe = upstreamObservable {
                switch $0 {
                case .next(let value):
                    observer.onNext(callback(value))
                case .error(let err):
                    observer.onError(err)
                default:
                    observer.onCompleted()
                }
            }
            return unsubscribe
        }
    }
}

print("Started")


var obs = timer(for: 3)
          => map{ $0 + " " +  "Mapped"}
          => map { $0.count }

var unsubscribe = obs {
    switch $0 {
    case .next(let value):
       print(value)
    case .error(let err):
        print(err)
    default:
       print("Completed")
    }
}

//print("Canceled")
//unsubscribe()
