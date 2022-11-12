#include <iostream>

using namespace std;

template<typename Functor>
void f(Functor functor)
{
    cout << functor(10) << endl;
}

int g(int x)
{
    return x * x;
}
int main()
{
    auto lambda = [] (int x) { cout << x * 50 << endl; return x * 100; };
    f(lambda); //pass lambda
    f(g);      //pass function 
}