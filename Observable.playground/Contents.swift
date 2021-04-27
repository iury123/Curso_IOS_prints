import UIKit

enum ObservableError: Error {
    case error
}

class Subscription {
    
    typealias Unsubscriber = () -> Void
    private let unsubscribeFunc: Unsubscriber?

    init(unsubscribeFunction: Unsubscriber? = nil) {
        unsubscribeFunc = unsubscribeFunction
    }
    
    func unsubscribe() {
        unsubscribeFunc?()
    }
}

protocol IObserver {
    associatedtype T
    func onNext(value: T) -> Void
    func onError(error: Error) -> Void
    func onComplete() -> Void
}

class Observer<T>: IObserver {
    
    typealias OnNext = (T) -> Void
    typealias OnError = (Error) -> Void
    typealias OnComplete = () -> Void
    
    private let onNext: OnNext?
    private let onError: OnError?
    private let _onComplete: OnComplete?
    private var streamFinished = false
    
    init(onNext: OnNext? = nil, onError: OnError? = nil, onComplete: OnComplete? = nil) {
        self.onNext = onNext
        self.onError = onError
        _onComplete = onComplete
    }
    
    func onNext(value: T) -> Void {
        if(!streamFinished) {
            onNext?(value)
        }
    }
    
    func onError(error: Error) -> Void {
        if(!streamFinished) {
            streamFinished = true
            onError?(error)
        }
    }
    
    func onComplete() -> Void {
        if(!streamFinished) {
            streamFinished = true
            _onComplete?()
        }
    }
}

protocol IObservable {
    associatedtype T
    func subscribe(observer: Observer<T>) -> Subscription
}

class Observable<T>: IObservable {

    typealias Subscribe = (Observer<T>) -> Subscription
    private let subscribe: Subscribe

    init(subscribe: @escaping Subscribe) {
        self.subscribe = subscribe
    }
    
    func subscribe(observer: Observer<T>) -> Subscription {
        subscribe(observer)
    }
}

class Subject<T>: IObservable, IObserver {
    
    func subscribe(observer: Observer<T>) -> Subscription {
        <#code#>
    }
    
    func onNext(value: T) {
        <#code#>
    }
    
    func onError(error: Error) {
        <#code#>
    }
    
    func onComplete() {
        <#code#>
    }
}

extension IObservable {
    
    func map<U>(_ mapper: @escaping (T) -> U) -> Observable<U> {
        Observable<U>() { observer in
            let subscription = subscribe(observer: Observer<T>(
                    onNext: { observer.onNext(value: mapper($0)) },
                    onError: { observer.onError(error: $0) },
                    onComplete: { observer.onComplete() }
                ))
            return subscription
        }
    }
    
    
    func filter(_ predicate: @escaping (T) -> Bool) -> Observable<T> {
        Observable<T>() { observer in
            let subscription = subscribe(observer: Observer<T>(
                    onNext: {
                        if(predicate($0)) {
                            observer.onNext(value: $0)
                        }
                    },
                    onError: { observer.onError(error: $0) },
                    onComplete: { observer.onComplete() }
                ))
            return subscription
        }
    }
    
    func debounce(seconds time: Double) -> Observable<T> {
        Observable<T>() { observer in
            var timer: Timer?
            var completed = false
            let subscription = subscribe(observer: Observer<T>(
                    onNext: { value in
                        timer?.invalidate()
                        timer = Timer.scheduledTimer(withTimeInterval: time, repeats: false) {_ in
                            observer.onNext(value: value)
                            if(completed) {
                                observer.onComplete()
                            }
                        }
                    },
                    onError: {
                        timer?.invalidate()
                        observer.onError(error: $0)
                    },
                    onComplete: {
                        completed = true
                        if(!(timer?.isValid ?? false)) {
                            observer.onComplete()
                        }
                    }
                ))
            return Subscription {
                timer?.invalidate()
                subscription.unsubscribe()
            }
        }
    }
}

let obs = Observable<Int>() { observer in
    observer.onNext(value: 1)
    observer.onNext(value: 2)
//    observer.onError(error: ObservableError.error)
    observer.onComplete()
    observer.onNext(value: 3)
    observer.onNext(value: 4)
    return Subscription { print("Unsubscribed") }
}

let subscription = obs
    .debounce(seconds: 2)
    .filter { $0 % 2 == 0 }
    .map { "Mapped \($0)" }
    .subscribe(observer:  Observer<String>(
        onNext: { print("OnNext: \($0)") },
        onError: { print("OnError: \($0)") },
        onComplete: { print("Completed") }
    ))

//subscription.unsubscribe()
print("Vai printar")
